require("dotenv").config()

const EventEmitter = require("events")

const bunyan = require("bunyan")
const Koa = require("koa")
const Router = require("@koa/router")
const _ = require("lodash")
const { ethers } = require("ethers")
const { Pool } = require("pg")
const sb = require("sql-bricks-postgres")
const { ulid } = require("ulid")

const IntervalMath = require("./intervals")


const logger = bunyan.createLogger({ name: "eth-indexer" })
logger.level("TRACE")

const port = Number(process.env.PORT)
const syncingLowerLimit = Number(process.env.SYNCING_LOWER_LIMIT || 0)
const syncingUpperLimit = Number(process.env.SYNCING_UPPER_LIMIT || Infinity)
const syncingStepSize = Number(process.env.SYNCING_STEP_SIZE || 100)
const syncingWhitelist = parseLogPredicate(process.env.SYNCING_WHITELIST)
const syncingBlacklist = parseLogPredicate(process.env.SYNCING_BLACKLIST)
const syncingStrategy = process.env.SYNCING_STRATEGY
const syncingLockDuration = process.env.SYNCING_LOCK_DURATION
const instanceId = process.env.INSTANCE_ID

logger.debug("whitelist: %o", syncingWhitelist)
logger.debug("blacklist: %o", syncingBlacklist)

function parseLogPredicate(str) {
  if (typeof str !== "string") {
    return parseLogPredicate("")
  }

  const predicates = str.split(",").map((a) => a.trim())

  const addressPredicates = predicates
    .filter((a) => a.startsWith("address:"))
    .map((a) => [a.slice("address:".length), true])

  const topic0Predicates = predicates
    .filter((a) => a.startsWith("topic0:"))
    .map((a) => [a.slice("topic0:".length), true])

  return {
    address: Object.fromEntries(addressPredicates),
    topic0: Object.fromEntries(topic0Predicates),
    isEmpty: addressPredicates.length === 0 && topic0Predicates.length === 0,
  }
}

function matchPredicate(log, predicate, matchEmpty = false) {
  if (predicate.isEmpty && matchEmpty) {
    return true
  } else {
    return predicate.address[log.address] || predicate.topic0[log.topics[0]]
  }
}

function extractRows(result) {
  return result.rows.map((obj) => _.mapKeys(obj, (_v, k) => _.camelCase(k)))
}

function objectSnakeCase(obj) {
  return _.mapKeys(obj, (_v, k) => _.snakeCase(k))
}

function logId({ transactionHash, blockNumber, logIndex }) {
  return (
    "B" +
    String(blockNumber).padStart(10, "0") +
    "L" +
    String(logIndex).padStart(5, "0") +
    "H" +
    transactionHash.slice(2, 10)
  )
}

function combineTopics(row) {
  const { topic0, topic1, topic2, topic3, ...rest } = row
  const topics = [row.topic0, row.topic1, row.topic2, row.topic3].filter(
    (x) => x
  )

  return { ...rest, topics }
}

async function insertLogs(pool, logs) {
  const mappedLogs = logs
    .filter((item) => matchPredicate(item, syncingWhitelist, true) && !matchPredicate(item, syncingBlacklist, false))
    .map((item) => {
      const { topics, ...rest } = item

      return {
        ...rest,
        insertionId: ulid(),
        topic0: topics[0],
        topic1: topics[1],
        topic2: topics[2],
        topic3: topics[3],
      }
    })
    .map((item) => ({ ...item, id: logId(item) }))

  for (const chunk of _.chunk(mappedLogs, 100)) {
    const stmt = sb
      .insert("log", chunk.map(objectSnakeCase))
      .onConflict()
      .doNothing()
    await pool.query(stmt.toParams())
  }

  return mappedLogs.length
}

function pickRangeByStretegy(ranges, strategy) {
  if (strategy === "FROM_EARLIEST") {
    const r = ranges[0]
    return { start: r.start, end: Math.min(r.end, r.start + syncingStepSize) }
  } else if (strategy === "FROM_LATEST") {
    const r = ranges[ranges.length - 1]
    return { start: Math.max(r.start, r.end - syncingStepSize), end: r.end }
  } else {
    throw new Error("invalid strategy")
  }
}

function rangesFromRows(rows) {
  return rows
    .map(it => ({ start: it.from_block, end: it.to_block }))
    .reduce((acc, it) => IntervalMath.union(acc, [it]), [])
}

async function getUnavailableRanges(client) {
  const result = await client.query(`
    SELECT from_block, to_block FROM completed_range
    UNION
    SELECT from_block, to_block FROM locked_range
  `)

  return rangesFromRows(result.rows)
}

async function pickWorkingRange(pool, listener) {
  while (true) {
    const fullRanges = [{ start: syncingLowerLimit, end: Math.min(listener.block, syncingUpperLimit) }]
    const unavailableRanges = await getUnavailableRanges(pool)
    const availableRanges = IntervalMath.difference(fullRanges, unavailableRanges)
    logger.trace("pick working range %o : %o : %o", fullRanges, unavailableRanges, availableRanges)

    if (availableRanges.length === 0) {
      await listener.wait()
      continue
    }

    return pickRangeByStretegy(availableRanges, syncingStrategy)
  }
}

async function acquireLock(pool, range) {
  const client = await pool.connect()

  try {
    await client.query("BEGIN")
    await client.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

    const unavailableRanges = await getUnavailableRanges(client)
    const isOverlapFree = unavailableRanges.every(it => !IntervalMath.isOverlap(it, range))
    if (!isOverlapFree) {
      await client.query("ROLLBACK")
      logger.warn("failed to acquire lock because of overlapping")
      return false
    }

    await client.query(`
      INSERT INTO locked_range(instance_id, created_at, expires_at, from_block, to_block)
      VALUES ($1, $2, $3, $4, $5)
    `, [instanceId, new Date(), new Date(new Date() + syncingLockDuration * 1000), range.start, range.end])
    
    await client.query("COMMIT")
    return true

  } catch (e) {
    await client.query("ROLLBACK")
    logger.warn(e, "acquire a range lock failed")
    return false

  } finally {
    client.release()
  }
}

async function updateCheckpoint(pool, range) {
  const client = await pool.connect()

  try {
    await client.query("BEGIN")
    await client.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

    const result = await client.query("SELECT from_block, to_block FROM completed_range")
    const completedRanges = rangesFromRows(result.rows)
    const newCompletedRanges = IntervalMath.union(completedRanges, [range])

    await client.query("DELETE FROM completed_range")
    for (const r of newCompletedRanges) {
      await client.query(`
        INSERT INTO completed_range(from_block, to_block)
        VALUES ($1, $2)
      `, [r.start, r.end])
    }

    await client.query("COMMIT")
  } catch (e) {
    await client.query("ROLLBACK")
    throw e

  } finally {
    client.release()
  }
}

async function fetchAndSaveLogs(pool, provider, fromBlock, toBlock) {
  console.time(`download logs ${fromBlock} to ${toBlock}`)
  const logs = await provider.getLogs({ fromBlock, toBlock })
  console.timeEnd(`download logs ${fromBlock} to ${toBlock}`)

  const c = await insertLogs(pool, logs)
  logger.info(
    `downloaded ${logs.length} log entries from block ${fromBlock} to ${toBlock} (${logs.length - c} log entries ignored)`
  )
}

async function releaseLock(pool) {
  const result = await pool.query("DELETE FROM locked_range WHERE instance_id = $1", [instanceId])
  if (result.rowCount > 0) {
    logger.debug("range lock released")
  }
}

async function mainloop(pool, provider, listener) {
  await listener.initialize()

  while (true) {
    await releaseLock(pool)

    const range = await pickWorkingRange(pool, listener)
    logger.trace("picked working range %o", range)

    const success = await acquireLock(pool, range)
    if (!success) continue
    logger.debug("range lock acquired %o", range)

    await fetchAndSaveLogs(pool, provider, range.start, range.end)
    await updateCheckpoint(pool, range)

    await releaseLock(pool)
  }
}

async function startHttpServer(pool) {
  const app = new Koa()
  const router = new Router()

  router.get("/api/v1/logs", async (ctx) => {
    const { address, cursor } = ctx.query
    const limit = Number(ctx.query.limit) || 100

    const stmt = sb.select("*").from("log")

    if (address) {
      stmt.where("address", address)
    }

    if (cursor) {
      const insertionId = Buffer.from(cursor, "base64").toString("ascii")
      stmt.where(sb.gt("insertion_id", insertionId))
    }

    stmt.orderBy("insertion_id ASC")

    stmt.limit(Math.max(Number(limit), 100))

    const s = stmt.toParams()
    logger.debug("sql dump %j", s)

    const rows0 = extractRows(await pool.query(s)).slice(0, limit)
    const rows1 = rows0.map((r) => _.omit(r, "id")).map(combineTopics)

    const nextCursor0 = rows0?.[0]?.insertionId
    const nextCursor1 = nextCursor0
      ? Buffer.from(nextCursor0).toString("base64")
      : null

    ctx.body = { items: rows1, nextCursor: nextCursor1 }
  })

  app.use(router.routes())
  app.listen(port)
  logger.info(
    { what: "http.start", port },
    "http server started at port %s",
    port
  )
}

class BlockListener {

  constructor(provider) {
    this.provider = provider
    this.block = -1
    this.events = new EventEmitter()

    this.provider.on("block", (b) => {
      this.block = b
      logger.trace("block mined %s", b)
      this.events.emit("block")
    })
  }

  wait(timeout = 30_000) {
    return new Promise((resolve) => {
      setTimeout(resolve, timeout)
      this.events.once("block", resolve)
    })
  }

  async initialize() {
    if (this.block === -1) {
      await this.wait()
    }
  }

}

async function main() {
  logger.info("service eth-indexer is starting")
  logger.info(`syncing from ${syncingLowerLimit} to ${syncingUpperLimit}`)
  const pool = new Pool()

  const httpProvider = new ethers.providers.JsonRpcProvider(
    process.env.NODE_RPC_HTTP_ENDPOINT
  )
  const websocketProvider = new ethers.providers.WebSocketProvider(
    process.env.NODE_RPC_WEBSOCKET_ENDPOINT
  )
  const listener = new BlockListener(websocketProvider)

  startHttpServer(pool)

  await mainloop(pool, httpProvider, listener)
}

main().catch((e) => {
  console.error(e)
  process.exit(-1)
})

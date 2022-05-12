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

const logger = bunyan.createLogger({ name: "eth-indexer" })
logger.level("TRACE")

const port = Number(process.env.PORT)
const syncingLowerLimit = Number(process.env.SYNCING_LOWER_LIMIT || 0)
const syncingUpperLimit = Number(process.env.SYNCING_UPPER_LIMIT || Infinity)
const syncingStepSize = Number(process.env.SYNCING_STEP_SIZE || 100)
const syncingWhitelist = parseLogPredicate(process.env.SYNCING_WHITELIST)
const syncingBlacklist = parseLogPredicate(process.env.SYNCING_BLACKLIST)

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

function waitForNewBlock(events) {
  return new Promise((resolve) => {
    events.once("block", (b) => resolve(b))
  })
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

async function initializeCheckpoint(pool, provider) {
  const result = await pool.query("SELECT * FROM checkpoint")
  if (result.rows.length === 0) {
    const b = await provider.getBlockNumber()

    await pool.query(
      `
      INSERT INTO checkpoint(downloaded_lower_bound, downloaded_upper_bound, published_lower_bound, published_upper_bound)
      VALUES ($1, $2, $3, $4)
    `,
      [b - 1, b, b - 1, b]
    )
    logger.info("initialized the checkpoint to block", b)
  }
}

async function syncBackward(pool, provider) {
  while (true) {
    const rows = extractRows(
      await pool.query("SELECT downloaded_lower_bound FROM checkpoint")
    )
    const lowerBound = rows[0].downloadedLowerBound

    if (lowerBound < syncingLowerLimit) {
      logger.info("backward syncing completed")
      return
    }

    const fromBlock = Math.max(lowerBound - syncingStepSize, syncingLowerLimit)
    const toBlock = lowerBound
    const newLowerBound = fromBlock - 1

    console.time(`download logs ${fromBlock} to ${toBlock}`)
    const logs = await provider.getLogs({ fromBlock, toBlock })
    console.timeEnd(`download logs ${fromBlock} to ${toBlock}`)

    const c = await insertLogs(pool, logs)
    logger.info(
      `downloaded ${logs.length} log entries from block ${fromBlock} to ${toBlock} (${logs.length - c} log entries ignored)`
    )

    await pool.query("UPDATE checkpoint SET downloaded_lower_bound = $1", [
      newLowerBound,
    ])
  }
}

async function syncForward(pool, provider, events) {
  let blockHeight = await provider.getBlockNumber()

  while (true) {
    const rows = extractRows(
      await pool.query("SELECT downloaded_upper_bound FROM checkpoint")
    )
    const upperBound = rows[0].downloadedUpperBound

    if (upperBound > syncingUpperLimit) {
      logger.info("forward syncing completed")
      return
    }

    if (upperBound > blockHeight) {
      logger.debug("reach current block height, waiting for new block")
      blockHeight = await waitForNewBlock(events)
      continue
    }

    const fromBlock = upperBound
    const toBlock = Math.min(
      upperBound + syncingStepSize,
      Math.min(blockHeight, syncingUpperLimit)
    )
    const newUpperBound = toBlock + 1

    console.time(`download logs ${fromBlock} to ${toBlock}`)
    const logs = await provider.getLogs({ fromBlock, toBlock })
    console.timeEnd(`download logs ${fromBlock} to ${toBlock}`)

    const c = await insertLogs(pool, logs)
    logger.info(
      `downloaded ${logs.length} log entries from block ${fromBlock} to ${toBlock} (${logs.length - c} log entries ignored)`
    )

    await pool.query("UPDATE checkpoint SET downloaded_upper_bound = $1", [
      newUpperBound,
    ])
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

async function main() {
  logger.info("service eth-indexer is starting")
  logger.info(`syncing from ${syncingLowerLimit} to ${syncingUpperLimit}`)
  const pool = new Pool()

  const httpProvider = new ethers.providers.JsonRpcProvider(
    process.env.NODE_RPC_HTTP_ENDPOINT
  )
  const wsProvider = new ethers.providers.WebSocketProvider(
    process.env.NODE_RPC_WEBSOCKET_ENDPOINT
  )
  const events = new EventEmitter()

  wsProvider.on("block", (b) => {
    logger.trace("got block event", b)
    events.emit("block", b)
  })

  await initializeCheckpoint(pool, httpProvider)

  logger.trace("eth-indexer initialized")

  startHttpServer(pool)

  await Promise.all([
    syncForward(pool, httpProvider, events),
    syncBackward(pool, httpProvider),
  ])
}

main().catch((e) => {
  console.error(e)
  process.exit(-1)
})

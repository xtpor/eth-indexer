
# Run development postgres

```
docker run -d --network development --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:14
```

# Run development adminer

```
docker run -d --network development --rm --name adminer -p 18080:8080 adminer
```

# Environment

```
INSTANCE_ID=b
PORT=3002

PGHOST=localhost
PGUSER=postgres
PGDATABASE=eth_indexer
PGPASSWORD=password
PGPORT=5432

NODE_RPC_HTTP_ENDPOINT=https://speedy-nodes-nyc.moralis.io/-/polygon/mumbai
NODE_RPC_WEBSOCKET_ENDPOINT=wss://speedy-nodes-nyc.moralis.io/-/polygon/mumbai/ws

SYNCING_STRATEGY=FROM_EARLIEST
SYNCING_LOCK_DURATION=30000
SYNCING_LOWER_LIMIT=26290000
SYNC_UPPER_LIMIT=30000000
SYNCING_STEP_SIZE=100
SYNCING_WHITELIST=topic0:0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,address:0x55B2BA8AF2675eeFDFa73A2D8E6df7aBe80BbC03
SYNCING_BLACKLIST=address:0x0000000000000000000000000000000000001010
```
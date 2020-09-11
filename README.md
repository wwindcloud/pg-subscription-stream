# pg-subscription-stream
PG Subscription Stream - Subscribing to a PG logical replication slot and receive database changes

## Installation

```sh
$ npm install pg-subscription-stream
```

## What is this?
Let's say you want to receive notification of some tables from PG when there's any changes made upon. It's quite possible to setup an event trigger on the tables and do a `LISTEN/NOTIFY` dance in order to receive the changes, but this can be quite tedious and error proned. What if your receiving side has a network problem, then all changes during that period will be lost. Fortunately, PG does have a solution by using `PUBLICATION` and `SUBSCRIPTION`, it's possible to track changes and gurantee the receiving side received everything before moving forward. And this is the purpose of this library, by emulating `pg_recvlogical`, it helps your node program to subscribe to multiple `PUBLICATION` in PG using logical replication slot and start receiving table changes in stream.

## How to use this library?

### PostgreSQL server side
```sql
CREATE PUBLICATION my_publication FOR TABLE my_table;

SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

### Node application side
```js
const {Client, types} = require('pg')
const {PgSubscriptionStream, PgOutputParser} = require('pg-subscription-stream')
const pipeline = require('util').promisify(require('stream').pipeline)
const {Writable} = require('stream')

const client = new Client({
  connectionString: 'postgresql://localhost:5432',
  replication: 'database'
})

;(async () => {
  await client.connect()
  
  // Prepare to receive logical replication stream
  const stream = client.query(new PgSubscriptionStream({
    slotName: 'my_slot',
    pluginOptions: {
      proto_version: 1,
      publication_names: 'my_publication'
    }
  }))
  
  // A Parser to decode the output from server side logical decoding plugin
  const parser = new PgOutputParser({
    typeParsers: types,
    includeLsn: true
  })
  
  // Pipeline to a Writable stream
  await pipeline(
    stream,
    parser,
    new Writable({
      objectMode: true,
      write: (chunk, encoding, cb) => {
        const {kind, schema, table, KEY, OLD, NEW} = chunk
        
        // Write to your desintation, do your stuff...
        console.log(chunk)
        cb()
      }
    })
  )
    
  // Or using async iterator
  for await (const chunk of stream.pipe(parser)) {
    const {kind, schema, table, KEY, OLD, NEW} = chunk
    // Do your stuff
  }
})()


```

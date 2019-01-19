## ioredisgraph ##

A javascript client for [RedisGraph](https://oss.redislabs.com/redisgraph/) based of of the excellent [ioredis](https://github.com/luin/ioredis) client.
Literally based off ioredis. This module wraps ioredis, adds a couple commands, and returns an array of objects from each query (as well as the query meta information).

```
let RedisGraph = require('./');

async function demo () {
  let graph = new RedisGraph('MotoGP')
  await graph.query("CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}), (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}), (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})")
  let ridingForYamaha = await graph.query("MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = 'Yamaha' RETURN r,t")

  console.log(ridingForYamaha)
  // [ { 'r.name': 'Valentino Rossi', 't.name': 'Yamaha' },
  // meta: { queryInternalExecutionTime: '1.446600 milliseconds' } ]

  await graph.delete()
}

demo()
```

### Usage ###

#### Constructor ####

ioredisgraph has all the same constructor options as ioredis, but adds a `graphName` argument.


Add graph name as first argument to constructor:
`let graph = new RedisGraph('DEMO_GRAPH')`
`let graph = new RedisGraph('DEMO_GRAPH', 6379)`
or add to options object:
`let graph = new RedisGraph({graphName: 'DEMO_GRAPH', port: 6379})`

#### Methods ####
These methods are added. All other methods usually available to an ioredis client are accessible.

__graph.query(cypherString)__
Pass in a string for your cypher query.
Results are an array of object. Each object has properties based on the name used in the query.
access `results.meta` for the meta information redisgraph returns.

__graph.delete()__
Deletes database

__graph.explain(cypherString)__

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

let test = require('ava')

let RedisGraph = require('./');

test('ping', async (t) => {
  let graph = new RedisGraph('test')
	t.true(await graph.ping() === "PONG")
})

test('missing graph name', async t => {
  t.throws(() => new RedisGraph())
})

test('query, and meta response', async (t) => {
  let graph = new RedisGraph('test')
  let result = await graph.query(`CREATE (:person {name: 'Chuckwudi'})`)
  t.log(result)
  t.true(result.meta.nodesCreated === '1')
})

test('options constructor', async (t) => {
  let graph = new RedisGraph({graphName: 'test'})
  t.truthy(await graph.query(`CREATE (:person {name: 'Chuckwudi'})`) instanceof Array)
})

test.only('multiple instances stay separate', async (t) => {
  let a = new RedisGraph('a')
  let b = new RedisGraph('b')
  t.truthy(a.graphName == 'a')
  t.truthy(b.graphName == 'b')
})

test('response node parsing', async (t) => {
  let graph = new RedisGraph('nodeResponse')
  await graph.query(`CREATE (:person {name: 'Chuck'}), (:person {name: 'Austin'}), (:person {name: 'Zack'})`)
  let result = await graph.query(`MATCH (a:person) RETURN a, id(a)`)
//  console.log(JSON.stringify(result, null, 2))
  t.truthy(result.length > 2)
  t.truthy(result[1]['a']['id'])
  t.truthy(result[1]['a']['name'])
  t.truthy(result[1]['a']['labels'][0] == 'person')
  t.truthy(result[1]['id(a)'])
})

test('response relation parsing', async (t) => {
  let graph = new RedisGraph('relationResponse')
  await graph.query(`CREATE (:person {name: 'Chuck'})-[:friendsWith]->(:person {name: 'Austin'})`)
  let result = await graph.query(`MATCH (:person)-[r:friendsWith]->(:person) RETURN r, id(r)`)
//  console.log(JSON.stringify(result, null, 2))
  t.truthy(result.length > 0)
  t.truthy(!isNaN(result[0]['r']['id']))
  t.truthy(result[0]['r']['labels'][0] == 'friendsWith')
  t.truthy(!isNaN(result[0]['id(r)']))
})

test.skip('delete graph', async (t) => {
  let graph = new RedisGraph('delete')
  await graph.query(`CREATE (:person {name: 'Chuckwudi'})`)
  t.truthy(await graph.delete())
})

test.skip('explain', async (t) => {
  let graph = new RedisGraph('test')
  t.log(await graph.explain(`CREATE (:person {name: 'Chuckwudi'})`))
  t.truthy(await graph.explain(`CREATE (:person {name: 'Chuckwudi'})`))
})

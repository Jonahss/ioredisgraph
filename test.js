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

test('response parsing', async (t) => {
  let graph = new RedisGraph('response')
  await graph.query(`CREATE (:person {name: 'Chuck'}), (:person {name: 'Austin'}), (:person {name: 'Zack'})`)
  let result = await graph.query(`MATCH (a:person) RETURN a, id(a)`)
  t.truthy(result.length > 2)
  t.truthy(result[0]['a.name'])
  t.truthy(result[0]['id(a)'])
})

test('delete graph', async (t) => {
  let graph = new RedisGraph('delete')
  await graph.query(`CREATE (:person {name: 'Chuckwudi'})`)
  t.truthy(await graph.delete())
})

test('explain', async (t) => {
  let graph = new RedisGraph('test')
  t.log(await graph.explain(`CREATE (:person {name: 'Chuckwudi'})`))
  t.truthy(await graph.explain(`CREATE (:person {name: 'Chuckwudi'})`))
})


//test('')

let Redis = require('ioredis')
let camelCase = require('camelcase')
let _ = require('lodash')

class RedisGraph extends Redis {
  constructor(graphName, ...args) {
    if (typeof graphName === 'object') {
      super(graphName)
      this.graphName = graphName.graphName
    } else {
      super(...args)
      this.graphName = graphName
    }

    if (!this.graphName || this.graphName.length < 1) {
      throw new Error("Must specify a graph name in constructor")
    }

    // Here's the built-in reply transformer converting the HGETALL reply
    // ['k1', 'v1', 'k2', 'v2']
    // into
    // { k1: 'v1', 'k2': 'v2' }
    Redis.Command.setReplyTransformer('GRAPH.QUERY', function (result) {
      let resultKey = result[0].shift()
      let resultSet = result[0]
      
      resultSet = resultSet.map((result) => {
        result = result.map((value, index) => {
          return [resultKey[index], value]
        })
        return _.fromPairs(result)
      })
      resultSet.meta = parseMetaInformation(result[1])

      return resultSet
    });
  }

  async query (command) {
    return this.call('GRAPH.QUERY', this.graphName, `${command}`)
  }

  async delete () {
    return this.call('GRAPH.DELETE', this.graphName)
  }

  async explain (command) {
    return this.call('GRAPH.EXPLAIN', this.graphName, `${command}`)
  }
}

function parseMetaInformation (array) {
  meta = {}
  for (prop of array) {
    let [name, value] = prop.split(': ')
    value = value.trim()
    name = camelCase(name)
    meta[name] = value
  }
  return meta
}

module.exports = RedisGraph

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

    // A single query returns an array with up to 3 elements:
    //  - the column names for each result
    //  - an array of result objects
    //  - some meta information about the query
    // A single result can be a node, relation, or scalar in the case of something like (id(node))
    Redis.Command.setReplyTransformer('GRAPH.QUERY', function (result) {
      let metaInformation = parseMetaInformation(result.pop())

      let parsedResults = []
      parsedResults.meta = metaInformation

      if (result.length) { // if there are results to parse
        let columnHeaders = result[0]
        let resultSet = result[1]

        parsedResults = resultSet.map((result) => {
          return parseResult(columnHeaders, result)
        })
      }

      return parsedResults
    })
  }

  query (command) {
    return this.call('GRAPH.QUERY', this.graphName, `${command}`)
  }

  delete () {
    return this.call('GRAPH.DELETE', this.graphName)
  }

  explain (command) {
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

// a single result will consist of an array with one element for each returned object in the original QUERY
// for example: "... RETURN n, l, p" <- will return multiple rows/records, each of which will have n, l, and p.
function parseResult (columnHeaders, singleResult) {
  columns = columnHeaders.map((columnHeader, index) => {
    let name = columnHeader
    let value = singleResult[index]

    if (Array.isArray(value)) {
      value = _.fromPairs(value)
    }

    if (value.properties) {
      _.defaults(value, _.fromPairs(value.properties))
      delete value.properties
    }

    return [name, value]
  })

  return _.fromPairs(columns)
}


// add methods to Pipeline

Redis.Pipeline.prototype.query = function (command) {
  return this.call('GRAPH.QUERY', this.redis.graphName, `${command}`)
}

Redis.Pipeline.prototype.delete = function (command) {
  return this.call('GRAPH.DELETE', this.redis.graphName)
}

Redis.Pipeline.prototype.explain = function (command) {
  return this.call('GRAPH.EXPLAIN', this.redis.graphName, `${command}`)
}

module.exports = RedisGraph

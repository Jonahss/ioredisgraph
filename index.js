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
    });
  }

  // queries add the `--compact` option to be more efficient with regards to bandwidth and lookup on the redis server side
  async query (command) {
    return this.call('GRAPH.QUERY', this.graphName, `${command}`, '--compact')
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

function resolveComunTypeEnum (columnTypeEnum) {
  switch (columnTypeEnum) {
    case 1:
      return 'scalar'
    case 2:
      return 'node'
    case 3:
      return 'relation'
    default:
      return 'unknown'
  }
}

// a single result will consist of an array with one element for each returned object in the original QUERY
// for example: "... RETURN n, l, p" <- will return multiple rows/records, each of which will have n, l, and p.
function parseResult (columnHeaders, singleResult) {
console.log('parseResult', 'headers:', columnHeaders, 'singleresult:', singleResult)
  columns = columnHeaders.map((columnHeader, index) => {
    let resultType = resolveComunTypeEnum(columnHeader[0])
    let name = columnHeader[1]

    switch(resultType) {
      case 'scalar':
        return [name, parseScalar(singleResult[index])]
      case 'node':
        return [name, parseNode(singleResult[index])]
      case 'relation':
        return [name, parseRelationship(singleResult[index])]
      default:
        console.error(`unrecognized result with column hearder enum: ${columnHeader[0]}, named: ${name}`)
        return [name, null]
    }
  })

  return _.fromPairs(columns)
}

function parseScalar (unparsedScalar) {
  return unparsedScalar[1]
}

function parseNode (unparsedNode) {
  console.log(JSON.stringify(unparsedNode))

}

module.exports = RedisGraph

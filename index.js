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

  async query (command) {
    return this.call('GRAPH.QUERY', this.graphName, `${command}`)
  }

  async delete () {
    return this.call('GRAPH.DELETE', this.graphName)
  }

  async explain (command) {
    return this.call('GRAPH.EXPLAIN', this.graphName, `${command}`)
  }

  async resolveLabelName (labelId) {
    let labelName = (await this.labelNames)[labelId]
    if (labelName) {
      return labelName
    }
    // otherwise, we need to hydrate the label names from the server
    // let's have any other callers wait until we're finished
    await this.loadLabelNames()

    return (await this.labelNames)[labelId]
  }

  async loadLabelNames () {
    // if we're already loading, return the promise for the results
    if (this._loadingLabelNames) {
      return this._loadingLabelNames
    }
    // let's only do this once, have others wait
    this._loadingLabelNames = new Promise(async (resolve) => {
      console.log('loading label names')
      let labelNames = await this.query(`CALL db.labels()`)
      this.labelNames = labelNames.map((prop) => prop.label)
      this._loadingLabelNames = false
      console.log('label names loaded', labelNames, this.graphName)
      resolve()
    })

    return this._loadingLabelNames
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

module.exports = RedisGraph

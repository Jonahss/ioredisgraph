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

    // we use a special stored procedure to get these names, since the server responds with just index numbers for performance reasons
    this.propertyNames = []
    this.labelNames = Promise.resolve([])
    this.relationshipTypes = []

    this._loadingLabelNames = false

    this.resolvePropertyName = resolvePropertyName.bind(this)
    this.resolveRelationshipType = resolveRelationshipType.bind(this)
    this.parseNode = parseNode.bind(this)
    this.parseRelation = parseRelation.bind(this)
    this.parseResult = parseResult.bind(this)

    // A single query returns an array with up to 3 elements:
    //  - the column names for each result
    //  - an array of result objects
    //  - some meta information about the query
    // A single result can be a node, relation, or scalar in the case of something like (id(node))
    Redis.Command.setReplyTransformer('GRAPH.QUERY', async function (result) {
      let metaInformation = parseMetaInformation(result.pop())

      let parsedResults = []
      parsedResults.meta = metaInformation

      if (result.length) { // if there are results to parse
        let columnHeaders = result[0]
        let resultSet = result[1]

        parsedResults = resultSet.map(async (result) => {
          return this.parseResult(columnHeaders, result)
        })
        parsedResults = await Promise.all(parsedResults)
      }

      return parsedResults
    }.bind(this))
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
async function parseResult (columnHeaders, singleResult) {
console.log('parseResult', 'headers:', columnHeaders, 'singleresult:', singleResult, 'graphname', this.graphName)
  columns = columnHeaders.map(async (columnHeader, index) => {
    let resultType = resolveComunTypeEnum(columnHeader[0])
    let name = columnHeader[1]

    switch(resultType) {
      case 'scalar':
        return [name, parseScalar(singleResult[index])]
      case 'node':
        return [name, await this.parseNode(singleResult[index])]
      case 'relation':
        return [name, await this.parseRelation(singleResult[index])]
      default:
        console.error(`unrecognized result with column hearder enum: ${columnHeader[0]}, named: ${name}`)
        return [name, null]
    }
  })

  columns = await Promise.all(columns)

  return _.fromPairs(columns)
}

function parseScalar (unparsedScalar) {
  return unparsedScalar[1]
}

async function parseNode (unparsedNode) {
  let node = {
    id: unparsedNode[0],
    labels: await Promise.all(
      unparsedNode[1].map(async (labelId) => {
        return this.resolveLabelName(labelId)
      })
    ),
  }
  let properties = unparsedNode[2].map(async (prop) => {
    return [await this.resolvePropertyName(prop[0]), prop[2]]
  })
  properties = await Promise.all(properties)

  _.defaults(node, _.fromPairs(properties))

  return node
}

async function parseRelation (unparsedRelation) {
  let relation = {
    id: unparsedRelation[0],
    labels: [
      await this.resolveRelationshipType(unparsedRelation[1])
    ],
    sourceNodeId: unparsedRelation[2],
    destinationNodeId: unparsedRelation[3],
  }
  let properties = unparsedRelation[4].map(async (prop) => {
    return [await this.resolvePropertyName(prop[0]), prop[2]]
  })
  properties = await Promise.all(properties)

  _.defaults(relation, _.fromPairs(properties))

  return relation
}

async function resolvePropertyName (propertyNameId) {
  if (!this.propertyNames[propertyNameId]) {
    let propertyNames = await this.query(`CALL db.propertyKeys()`)
    this.propertyNames = propertyNames.map((prop) => prop.propertyKey)
    console.log('property names', propertyNames)
  }

  return this.propertyNames[propertyNameId]
}

async function resolveRelationshipType (relationshipTypeId) {
  if (!this.relationshipTypes[relationshipTypeId]) {
    let relationshipTypes = await this.query(`CALL db.relationshipTypes()`)
    this.relationshipTypes = relationshipTypes.map((prop) => prop.relationshipType)
    console.log('relationship types', relationshipTypes)
  }

  return this.relationshipTypes[relationshipTypeId]
}


module.exports = RedisGraph

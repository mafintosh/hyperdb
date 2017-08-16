var hyperdb = require('../..')
var ram = require('random-access-memory')

module.exports = create

create.one = createOne

function createOne (id) {
  return hyperdb(ram, {id: id, reduce: reduce, valueEncoding: 'json'})
}

function create (id) {
  return hyperdb(ram, {id: id, valueEncoding: 'json'})
}

function reduce (a, b) {
  return a
}

var hyperdb = require('../..')

module.exports = create

create.one = createOne

function createOne (id) {
  return hyperdb({id: id, reduce: reduce, valueEncoding: 'json'})
}

function create (id) {
  return hyperdb({id: id, valueEncoding: 'json'})
}

function reduce (a, b) {
  return a
}

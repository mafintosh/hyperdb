var hyperdb = require('../..')

module.exports = create

create.one = createOne

function createOne (id) {
  return hyperdb({id: id, reduce: reduce})
}

function create (id) {
  return hyperdb({id: id})
}

function reduce (a, b) {
  return a
}

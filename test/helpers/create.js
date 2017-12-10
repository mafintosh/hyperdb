var hyperdb = require('../..')
var ram = require('random-access-memory')

module.exports = create

create.one = createOne
create.two = createTwo
create.three = createThree

function createOne (key) {
  return hyperdb(ram, key, {reduce: reduce, valueEncoding: 'json'})
}

function createTwo (cb) {
  var a = hyperdb(ram, {valueEncoding: 'json'})
  a.ready(function () {
    var b = hyperdb(ram, a.key, {valueEncoding: 'json'})
    b.ready(function () {
      a.authorize(b.local.key, function () {
        cb(a, b)
      })
    })
  })
}

function createThree (cb) {
  var a = hyperdb(ram, {valueEncoding: 'json'})
  a.ready(function () {
    var b = hyperdb(ram, a.key, {valueEncoding: 'json'})
    b.ready(function () {
      var c = hyperdb(ram, a.key, {valueEncoding: 'json'})
      c.ready(function () {
        a.authorize(b.local.key, function () {
          b.authorize(c.local.key, function () {
            cb(a, b, c)
          })
        })
      })
    })
  })
}

function create (id) {
  return hyperdb(ram, {id: id, valueEncoding: 'json'})
}

function reduce (a, b) {
  return a
}

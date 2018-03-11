var hyperdb = require('../../')
var ram = require('random-access-memory')
var replicate = require('./replicate')
var reduce = (a, b) => a

exports.one = function () {
  return hyperdb(ram, null, {reduce, valueEncoding: 'utf-8'})
}

exports.two = function (cb) {
  var a = hyperdb(ram, {valueEncoding: 'utf-8'})
  a.ready(function () {
    var b = hyperdb(ram, a.key, {valueEncoding: 'utf-8'})
    b.ready(function () {
      a.authorize(b.local.key, function () {
        replicate(a, b, function () {
          cb(a, b, replicate.bind(null, a, b))
        })
      })
    })
  })
}

exports.three = function (cb) {
  var a = hyperdb(ram, {valueEncoding: 'utf-8'})

  a.ready(function () {
    var b = hyperdb(ram, a.key, {valueEncoding: 'utf-8'})
    var c = hyperdb(ram, a.key, {valueEncoding: 'utf-8'})

    b.ready(function () {
      c.ready(function () {
        a.authorize(b.local.key)
        a.authorize(c.local.key, function () {
          replicateAll(function () {
            cb(a, b, c, replicateAll)
          })
        })
      })
    })

    function replicateAll (cb) {
      replicate(a, b, function (err) {
        if (err) return cb(err)
        replicate(b, c, function (err) {
          if (err) return cb(err)
          replicate(a, c, cb)
        })
      })
    }
  })
}

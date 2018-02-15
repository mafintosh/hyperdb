var hyperdb = require('../../')
var replicate = require('./replicate')
var reduce = (a, b) => a

exports.one = function () {
  return hyperdb({id: 0, reduce})
}

exports.two = function (cb) {
  var a = hyperdb({id: 0})
  var b = hyperdb({id: 1})
  cb(a, b, replicate.bind(null, a, b))
}

exports.three = function (cb) {
  var a = hyperdb({id: 0})
  var b = hyperdb({id: 1})
  var c = hyperdb({id: 2})

  cb(a, b, c, replicateAll)

  function replicateAll (cb) {
    replicate(a, b, function (err) {
      if (err) return cb(err)
      replicate(b, c, function (err) {
        if (err) return cb(err)
        replicate(a, c, cb)
      })
    })
  }
}

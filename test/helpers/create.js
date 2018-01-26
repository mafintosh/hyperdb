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

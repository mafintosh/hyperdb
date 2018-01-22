var hyperdb = require('../../')
var reduce = (a, b) => a

exports.one = function () {
  return hyperdb({id: 0, reduce})
}

exports.two = function (cb) {
  cb(hyperdb({id: 0}), hyperdb({id: 1}))
}

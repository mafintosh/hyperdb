var bench = require('nanobench')
var create = require('./create')

module.exports = function (tag, cb) {
  create(function (err, db) {
    if (err) throw err
    bench(tag, function (b) {
      return cb(b, db)
    })
  })
}

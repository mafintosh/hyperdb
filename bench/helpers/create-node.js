var p = require('path')
var os = require('os')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')

var hyperdb = require('../..')

var count = 0
var dir = p.join(os.tmpdir(), '' + Math.random(0, 100))
mkdirp.sync(dir)

module.exports = function create (cb) {
  var db = hyperdb(p.join(dir, '' + count++))
  db.ready(function (err) {
    if (err) return cb(err)
    return cb(null, db)
  })
}

process.on('exit', function () {
  rimraf.sync(dir)
})

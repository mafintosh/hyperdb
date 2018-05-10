var hyperdb = require('../../')
var ram = require('random-access-memory')
var latency = require('random-access-latency')
var replicate = require('./replicate')
var reduce = (a, b) => a

exports.one = function (key, opts) {
  if (!opts) opts = {}
  var options = Object.assign({
    reduce,
    valueEncoding: 'utf-8'
  }, opts)
  var storage = options.latency ? name => latency(options.latency, ram()) : ram
  return hyperdb(storage, key, options)
}

exports.two = function (opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  createMany(2, opts, function (err, dbs, replicateByIndex) {
    if (err) return cb(err)
    dbs.push(replicateByIndex.bind(null, [0, 1]))
    return cb.apply(null, dbs)
  })
}

exports.three = function (opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  createMany(3, opts, function (err, dbs, replicateByIndex) {
    if (err) return cb(err)
    dbs.push(replicateByIndex.bind(null, [0, 1, 2]))
    return cb.apply(null, dbs)
  })
}

exports.many = createMany

function createMany (count, opts, cb) {
  if (typeof opts === 'function') return createMany(count, {}, opts)

  var dbs = []
  var remaining = count - 1

  var options = Object.assign({}, { valueEncoding: 'utf-8' }, opts)

  var first = hyperdb(ram, options)
  first.ready(function (err) {
    if (err) return cb(err)
    dbs.push(first)
    insertNext()
  })

  function insertNext () {
    if (remaining === 0) {
      // After the databases have been created, replicate all the authorizations.
      return replicateByIndex(err => {
        if (err) return cb(err)
        return cb(null, dbs, replicateByIndex)
      })
    }
    var db = hyperdb(ram, first.key, options)
    db.ready(function (err) {
      if (err) return cb(err)
      first.authorize(db.local.key, function (err) {
        if (err) return cb(err)
        dbs.push(db)
        remaining--
        return insertNext()
      })
    })
  }

  function replicateByIndex (indices, cb) {
    if (typeof indices === 'function') {
      cb = indices
      indices = dbs.map((_, i) => i)
    }
    if (indices.length === 0) return cb()

    var pairs = []
    for (var i = 0; i < indices.length; i++) {
      for (var j = 1; j < indices.length; j++) {
        if (i !== j) pairs.push([i, j])
      }
    }

    var remaining = pairs.length
    doReplicate()

    function doReplicate () {
      if (remaining === 0) return cb(null)
      var pair = pairs[pairs.length - remaining--]
      replicate(dbs[pair[0]], dbs[pair[1]], function (err) {
        if (err) return cb(null)
        return doReplicate()
      })
    }
  }
}

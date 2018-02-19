var toStream = require('nanoiterator/to-stream')
var mutexify = require('mutexify')
var iterator = require('./lib/iterator')
var differ = require('./lib/differ')
var changes = require('./lib/changes')
var get = require('./lib/get')
var put = require('./lib/put')

module.exports = DB

function DB (opts) {
  if (!(this instanceof DB)) return new DB(opts)
  if (!opts) opts = {}

  this._id = opts.id || 0
  this._feeds = []
  this._feeds[this._id] = []
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._snapshot = false
  this._lock = mutexify()
}

DB.prototype.snapshot = function () {
  var snapshot = new DB({id: this._id})
  snapshot._feeds = this._feeds.map(f => f.slice(0))
  snapshot._snapshot = true
  return snapshot
}

DB.prototype.heads = function (cb) {
  var heads = []
  var actual = []
  var i

  for (i = 0; i < this._feeds.length; i++) {
    if (this._feeds[i] && this._feeds[i].length) {
      heads.push(this._feeds[i][this._feeds[i].length - 1])
    }
  }
  // TODO: this could prob be done in O(heads) instead of O(heads^2)
  for (i = 0; i < heads.length; i++) {
    if (isHead(heads[i], heads)) actual.push(heads[i])
  }

  process.nextTick(cb, null, actual)

  function isHead (node, list) {
    var clock = node.seq + 1
    for (var i = 0; i < list.length; i++) {
      var other = list[i]
      if (other === node) continue
      if (other.clock[node.feed] >= clock) return false
    }
    return true
  }
}

DB.prototype.put = function (key, val, cb) {
  if (!cb) cb = noop

  if (this._snapshot) {
    return process.nextTick(cb, new Error('Cannot put on a snapshot'))
  }

  var self = this

  key = normalizeKey(key)

  this._lock(function (release) {
    self.heads(function (err, heads) {
      if (err) return unlock(err)
      put(self, heads, key, val, unlock)
    })

    function unlock (err) {
      release(cb, err)
    }
  })
}

DB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)

  var self = this

  this.heads(function (err, heads) {
    if (err) return cb(err)
    get(self, heads, normalizeKey(key), opts, cb)
  })
}

// Used by ./lib/*
DB.prototype._getPointer = function (feed, seq, cb) {
  process.nextTick(cb, null, this._feeds[feed][seq])
}

// Used by ./lib/*
DB.prototype._getAllPointers = function (ptrs, cb) {
  var results = new Array(ptrs.length)
  var error = null
  var missing = results.length

  if (!missing) return process.nextTick(cb, null, results)

  for (var i = 0; i < ptrs.length; i++) {
    var ptr = ptrs[i]
    this._getPointer(ptr.feed, ptr.seq, onnode)
  }

  function onnode (err, node) {
    if (err) error = err
    else results[indexOf(ptrs, node)] = node
    if (--missing) return
    if (error) cb(error, null)
    else cb(null, results)
  }
}

function indexOf (ptrs, ptr) {
  for (var i = 0; i < ptrs.length; i++) {
    var p = ptrs[i]
    if (ptr.feed === p.feed && ptr.seq === p.seq) return i
  }
  return -1
}

DB.prototype.list = function (prefix, opts, cb) {
  if (typeof prefix === 'function') return this.list('', null, prefix)
  if (typeof opts === 'function') return this.list(prefix, null, opts)

  var ite = this.iterator(prefix, opts)
  var list = []

  ite.next(loop)

  function loop (err, nodes) {
    if (err) return cb(err)
    if (!nodes) return cb(null, list)
    list.push(nodes)
    ite.next(loop)
  }
}

DB.prototype.changes = function () {
  return changes(this)
}

DB.prototype.diff = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.diff(other, null, prefix)
  return differ(this, other || checkoutEmpty(this), prefix || '', opts)
}

function checkoutEmpty (db) {
  db = db.snapshot()
  db._feeds = []
  return db
}

DB.prototype.iterator = function (prefix, opts) {
  if (isOptions(prefix)) return this.iterator('', prefix)
  return iterator(this, normalizeKey(prefix || ''), opts)
}

DB.prototype.createChangesStream = function () {
  return toStream(this.changes())
}

DB.prototype.createDiffStream = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.createDiffStream(other, '', prefix)
  return toStream(this.diff(other, prefix, opts))
}

DB.prototype.createReadStream = function (prefix, opts) {
  return toStream(this.iterator(prefix, opts))
}

function isOptions (opts) {
  return typeof opts === 'object' && !!opts
}

function normalizeKey (key) {
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}

function noop () {}

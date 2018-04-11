var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var hash = require('./hash')
var options = require('./options')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  nanoiterator.call(this)

  this._db = db
  this._stack = [prefix ? hash(prefix, false) : []]
  this._recursive = opts.recursive !== false
  this._gt = !!opts.gt
  this._start = this._stack[0].length
  this._end = this._recursive ? Infinity : this._start + hash.LENGTH
  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)
  this._collisions = []
}

inherits(Iterator, nanoiterator)

Iterator.prototype._next = function (cb) {
  // only needed to handle collisions
  while (this._collisions.length) {
    var collision = this._collisions.pop()
    if (allDeletes(collision)) continue
    process.nextTick(cb, null, this._prereturn(collision))
    return
  }

  var self = this
  var path = this._stack.pop()

  if (!path) {
    process.nextTick(cb, null, null)
    return
  }

  this._db.get('', {path, prefix: true, map: false, reduce: false}, done)

  function done (err, nodes) {
    if (err) return cb(err)
    self._onnodes(path, nodes, cb)
  }
}

Iterator.prototype._onnodes = function (path, nodes, cb) {
  if (!nodes.length) return this._next(cb)

  var ptr = fastForward(nodes, path.length, path)

  if (ptr < this._end) {
    var gt = this._gt || !this._start
    var sortEnd = gt && this._start === ptr ? 4 : 5

    for (var i = 0; i < sortEnd; i++) {
      var sortValue = i === 4 ? 4 : 3 - i // 3, 2, 1, 0, 4
      if (!visitTrie(nodes, ptr, sortValue)) continue
      this._stack.push(path.concat(sortValue))
    }
  }

  if (nodes.length > 1) nodes.sort(byKey)

  var result = null
  for (var j = 0; j < nodes.length; j++) {
    var node = nodes[j]
    if (node.path.length !== ptr && ptr !== this._end) continue

    if (!result) result = []

    if (result.length && result[0].key !== node.key) {
      this._collisions.push(result)
      result = []
    }

    result.push(node)
  }

  if (result && !allDeletes(result)) return cb(null, this._prereturn(result))
  this._next(cb)
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

function byKey (a, b) {
  var k = b.key.localeCompare(a.key)
  return k || b.feed - a.feed
}

function allDeletes (nodes) {
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].value) return false
  }
  return true
}

function visitTrie (nodes, ptr, val) {
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i]
    var bucket = node.trie[ptr]
    if (bucket && bucket[val]) return true
    if (node.path[ptr] === val) return true
  }
  return false
}

function fastForward (nodes, ptr, path) {
  if (nodes.length > 1) return ptr
  var node = nodes[0]
  while (ptr < node.path.length) {
    if (node.trie[ptr]) return ptr
    path.push(node.path[ptr++])
  }
  return ptr
}

var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var path = require('./path')
var options = require('./options')

var SORT_GT = [3, 2, 1, 0, path.SEPARATE]
var SORT_GTE = [3, 2, 1, 0, path.SEPARATE, path.TERMINATE]

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  nanoiterator.call(this)

  this._db = db
  this._stack = [{
    path: prefix ? this._db._path(prefix, false) : [],
    node: null,
    i: 0
  }]

  this._recursive = opts.recursive !== false
  this._gt = !!opts.gt
  this._start = this._stack[0].path.length
  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)
  this._collisions = []

  this._prefix = prefix
  this._pending = 0
  this._error = null
}

inherits(Iterator, nanoiterator)

Iterator.prototype._pushPointer = function (ptr, i, cb) {
  var self = this
  var top = {path: null, node: null, i}

  this._pending++
  this._stack.push(top)
  this._db._getPointer(ptr.feed, ptr.seq, false, done)

  function done (err, node) {
    if (err) self._error = err
    else top.node = node
    if (--self._pending) return
    if (self._error) return cb(self._error)
    self._next(cb)
  }
}

Iterator.prototype._pushNode = function (node, i) {
  this._stack.push({
    path: null,
    node,
    i
  })
}

Iterator.prototype._pushPrefix = function (path, i, val) {
  this._stack.push({
    path: (i < path.length ? path.slice(0, i) : path).concat(val),
    node: null,
    i
  })
}

// fast case
Iterator.prototype._singleNode = function (top, cb) {
  var node = top.node

  for (var i = top.i; i < node.trie.length; i++) {
    if (!this._recursive && node.path[i - 1] === path.SEPARATE) break

    var bucket = i < node.trie.length && node.trie[i]
    if (!bucket) continue

    var val = node.path[i]
    var order = this._sortOrder(i)

    for (var j = 0; j < order.length; j++) {
      var sortValue = order[j]
      var values = sortValue < bucket.length && bucket[sortValue]

      if (sortValue === val) {
        if (values) this._pushPrefix(node.path, i, sortValue)
        else this._pushNode(node, i + 1)
        continue
      }

      if (!values) continue
      if (values.length > 1) this._pushPrefix(node.path, i, sortValue)
      else this._pushPointer(values[0], i + 1, cb)
    }

    return this._pending === 0
  }

  if (!node.value || !isPrefix(node.key, this._prefix)) return true
  cb(null, this._prereturn([node]))
  return false
}

// slow case
Iterator.prototype._multiNode = function (path, nodes, cb) {
  if (!nodes.length) return this._next(cb)
  if (nodes.length === 1) {
    this._pushNode(nodes[0], path.length)
    return this._next(cb)
  }

  var ptr = path.length

  var order = this._sortOrder(ptr)

  for (var i = 0; i < order.length; i++) {
    var sortValue = order[i]
    if (!visitTrie(nodes, ptr, sortValue)) continue
    this._pushPrefix(path, ptr, sortValue)
  }

  nodes = this._filterResult(nodes, ptr)
  if (nodes && !allDeletes(nodes)) return cb(null, this._prereturn(nodes))
  this._next(cb)
}

Iterator.prototype._filterResult = function (nodes, i) {
  var result = null

  for (var j = 0; j < nodes.length; j++) {
    var node = nodes[j]
    if (this._recursive && node.path.length !== i) continue
    if (!this._recursive && node.path[i - 1] !== path.SEPARATE) continue
    if (!isPrefix(node.key, this._prefix)) continue

    if (!result) result = []

    if (result.length && result[0].key !== node.key) {
      this._collisions.push(result)
      result = []
    }

    result.push(node)
  }

  return result
}

Iterator.prototype._next = function (cb) {
  var nodes = drain(this._collisions)
  if (nodes) return cb(null, this._prereturn(nodes))

  var top = null

  while (true) {
    top = this._stack.pop()
    if (!top) return cb(null, null)
    if (!top.node) break
    if (!this._singleNode(top, cb)) return
  }

  this._lookupPrefix(top.path, cb)
}

Iterator.prototype._lookupPrefix = function (path, cb) {
  var self = this

  this._db.get('', {path, prefix: true, map: false, reduce: false}, done)

  function done (err, nodes) {
    if (err) return cb(err)
    self._multiNode(path, nodes, cb)
  }
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

Iterator.prototype._sortOrder = function (i) {
  var gt = this._gt || !this._start
  return gt && this._start === i ? SORT_GT : SORT_GTE
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
    var bucket = ptr < node.trie.length && node.trie[ptr]
    if (bucket && bucket[val]) return true
    if (node.path[ptr] === val) return true
  }
  return false
}

function drain (collisions) {
  while (collisions.length) {
    var collision = collisions.pop()
    if (allDeletes(collision)) continue
    return collision
  }

  return null
}

function isPrefix (s, prefix) {
  if (!prefix) return true
  if (s.startsWith) return s.startsWith(prefix)
  return s.slice(0, prefix.length) === prefix
}

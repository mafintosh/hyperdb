var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var iterator = require('./iterator')
var options = require('./options')

module.exports = Differ

function Differ (db, otherDb, prefix, opts) {
  if (!(this instanceof Differ)) return new Differ(db, otherDb, prefix, opts)
  nanoiterator.call(this)

  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)

  this._left = iterator(db, prefix, opts)
  this._right = iterator(otherDb, prefix, opts)
  this._leftNodes = null
  this._rightNodes = null

  // do not map/reduce the iterators - we just reset them here
  // cause that is easy peasy instead of extending the options
  noMapReduce(this._left)
  noMapReduce(this._right)
}

inherits(Differ, nanoiterator)

Differ.prototype._next = function (cb) {
  var self = this

  this._nextLeft(function (err, l) {
    if (err) return cb(err)

    self._nextRight(function (err, r) {
      if (err) return cb(err)

      if (!r && !l) return cb(null, null)

      if (!r || !l) {
        self._leftNodes = self._rightNodes = null
        return cb(null, {left: self._prereturn(l), right: self._prereturn(r)})
      }

      var kl = l[0].key
      var kr = r[0].key

      if (kl === kr) {
        if (same(l, r)) return self._skip(cb)
        // update / conflict
        self._leftNodes = self._rightNodes = null
        return cb(null, {left: self._prereturn(l), right: self._prereturn(r)})
      }

      // sort keys
      var sl = l[0].path.join('') + '@' + kl
      var sr = r[0].path.join('') + '@' + kr

      if (sl < sr) { // move left
        self._leftNodes = null
        cb(null, {left: self._prereturn(l), right: null})
      } else { // move right
        self._rightNodes = null
        cb(null, {left: null, right: self._prereturn(r)})
      }
    })
  })
}

Differ.prototype._prereturn = function (nodes) {
  if (!nodes) return nodes
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) nodes = nodes.reduce(this._reduce)
  return nodes
}

Differ.prototype._skip = function (cb) {
  /*
  // TODO: this can be greatly simplified
  var map = new Map()

  this._left._workers.forEach(function (t) {
    t.stack.forEach(index)
  })
  this._right._workers.forEach(function (t) {
    t.stack.forEach(index)
  })
  this._left._workers.forEach(function (t) {
    t.stack = t.stack.filter(filter)
  })
  this._right._workers.forEach(function (t) {
    t.stack = t.stack.filter(filter)
  })

  function index (s) {
    if (!s.node) return
    var k = s.node.feed + '@' + s.node.seq + '@' + s.i
    map.set(k, 1 + (map.get(k) || 0))
  }

  function filter (s) {
    if (!s.node) return true
    var k = s.node.feed + '@' + s.node.seq + '@' + s.i
    return map.get(k) < 2
  }
  */
  this._leftNodes = this._rightNodes = null
  this._next(cb)
}

Differ.prototype._nextRight = function (cb) {
  if (this._rightNodes) return cb(null, this._rightNodes)
  var self = this
  this._right.next(function (err, nodes) {
    if (err) return cb(err)
    self._rightNodes = nodes
    cb(null, nodes)
  })
}

Differ.prototype._nextLeft = function (cb) {
  if (this._leftNodes) return cb(null, this._leftNodes)
  var self = this
  this._left.next(function (err, nodes) {
    if (err) return cb(err)
    self._leftNodes = nodes
    cb(null, nodes)
  })
}

function same (l, r) {
  if (l.length !== r.length) return false
  // TODO: sort order should be same, but should verify that
  for (var i = 0; i < l.length; i++) {
    var a = l[i]
    var b = r[i]
    if (a.feed !== b.feed || a.seq !== b.seq) return false
  }
  return true
}

function noMapReduce (ite) {
  // if the iterator options are updated we *have* to
  // update them here
  ite._map = ite._reduce = null
}

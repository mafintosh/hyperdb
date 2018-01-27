var iterator = require('./iterator')
var options = require('./options')

module.exports = Differ

function Differ (db, otherDb, prefix, opts) {
  if (!(this instanceof Differ)) return new Differ(db, otherDb, prefix, opts)

  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)

  this.left = iterator(db, prefix, opts)
  this.right = iterator(otherDb, prefix, opts)
  this.leftNodes = null
  this.rightNodes = null

  // do not map/reduce the iterators - we just reset them here
  // cause that is easy peasy instead of extending the options
  noMapReduce(this.left)
  noMapReduce(this.right)
}

Differ.prototype.next = function (cb) {
  var self = this

  this.nextLeft(function (err, l) {
    if (err) return cb(err)

    self.nextRight(function (err, r) {
      if (err) return cb(err)

      if (!r && !l) return cb(null, null)

      if (!r || !l) {
        self.leftNodes = self.rightNodes = null
        return cb(null, {left: self._prereturn(l), right: self._prereturn(r)})
      }

      var kl = l[0].key
      var kr = r[0].key

      if (kl === kr) {
        if (same(l, r)) return self._skip(cb)
        // update / conflict
        self.leftNodes = self.rightNodes = null
        return cb(null, {left: self._prereturn(l), right: self._prereturn(r)})
      }

      // sort keys
      var sl = l[0].path.join('') + '@' + kl
      var sr = r[0].path.join('') + '@' + kr

      if (sl < sr) { // move left
        self.leftNodes = null
        cb(null, {left: self._prereturn(l), right: null})
      } else { // move right
        self.rightNodes = null
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
  // TODO: this can be greatly simplified
  var map = new Map()

  this.left._workers.forEach(function (t) {
    t._stack.forEach(index)
  })
  this.right._workers.forEach(function (t) {
    t._stack.forEach(index)
  })
  this.left._workers.forEach(function (t) {
    t._stack = t._stack.filter(filter)
  })
  this.right._workers.forEach(function (t) {
    t._stack = t._stack.filter(filter)
  })

  function index (s) {
    var k = s.feed + '@' + s.seq + '@' + s.i
    map.set(k, 1 + (map.get(k) || 0))
  }

  function filter (s) {
    var k = s.feed + '@' + s.seq + '@' + s.i
    return map.get(k) < 2
  }

  this.leftNodes = this.rightNodes = null
  this.next(cb)
}

Differ.prototype.nextRight = function (cb) {
  if (this.rightNodes) return cb(null, this.rightNodes)
  var self = this
  this.right.next(function (err, nodes) {
    if (err) return cb(err)
    self.rightNodes = nodes
    cb(null, nodes)
  })
}

Differ.prototype.nextLeft = function (cb) {
  if (this.leftNodes) return cb(null, this.leftNodes)
  var self = this
  this.left.next(function (err, nodes) {
    if (err) return cb(err)
    self.leftNodes = nodes
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

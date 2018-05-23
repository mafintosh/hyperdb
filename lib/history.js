var nanoiterator = require('nanoiterator')
var inherits = require('inherits')

module.exports = Iterator

function Iterator (db, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, opts)
  nanoiterator.call(this)

  this._db = db
  this._reverse = !!(opts && opts.reverse)
  this._end = []
  this._nodes = []
}

inherits(Iterator, nanoiterator)

Iterator.prototype._open = function (cb) {
  var self = this

  this._db.heads(function (err, heads) {
    if (err) return cb(err)

    var writers = self._db._writers

    for (var i = 0; i < writers.length; i++) {
      self._end.push(highestClock(heads, i))
      self._nodes.push(null)
    }

    self._updateAll(cb)
  })
}

Iterator.prototype._updateAll = function (cb) {
  var self = this
  var missing = 0
  var error = null
  var writers = this._db._writers

  for (var i = 0; i < this._nodes.length; i++) {
    if (this._end[i] > 1 && !this._nodes[i]) {
      missing++
      writers[i].get(this._reverse ? this._end[i] - 1 : 1, onnode)
    }
  }

  if (!missing) cb(null)

  function onnode (err, node) {
    if (err) error = err
    else self._nodes[node.feed] = node
    if (!--missing) cb(error)
  }
}

Iterator.prototype._next = function (cb) {
  var node = this._reverse ? this._max() : this._min()
  if (!node) return process.nextTick(cb, null, null)

  if (this._reverse) this._pop(node, cb)
  else this._shift(node, cb)
}

Iterator.prototype._pop = function (node, cb) {
  var self = this
  var writers = self._db._writers
  var w = writers[node.feed]
  var seq = node.seq - 1

  if (seq <= 0) {
    this._nodes[node.feed] = null
    return process.nextTick(cb, null, node)
  }

  w.get(seq, function (err, next) {
    if (err) return cb(err)
    self._nodes[next.feed] = next
    cb(null, node)
  })
}

Iterator.prototype._shift = function (node, cb) {
  var self = this
  var writers = self._db._writers
  var w = writers[node.feed]
  var seq = node.seq + 1

  if (seq >= this._end[node.feed]) {
    this._nodes[node.feed] = null
    return process.nextTick(cb, null, node)
  }

  w.get(seq, function (err, next) {
    if (err) return cb(err)
    self._nodes[next.feed] = next
    cb(null, node)
  })
}

Iterator.prototype._compare = function (fn) {
  var node = null
  for (var i = 0; i < this._nodes.length; i++) {
    var t = this._nodes[i]
    if (!t || (node && !fn(t, node))) continue
    node = t
  }
  return node
}

Iterator.prototype._min = function () {
  return this._compare(lt)
}

Iterator.prototype._max = function () {
  return this._compare(gt)
}

function lt (a, b) {
  var clock = a.feed < b.clock.length ? b.clock[a.feed] : 0
  return a.seq + 1 < clock
}

function gt (a, b) {
  var clock = a.feed < b.clock.length ? b.clock[a.feed] : 0
  return a.seq + 1 > clock
}

function highestClock (heads, i) {
  var max = 0
  for (var j = 0; j < heads.length; j++) {
    if (heads[j].clock.length <= i) continue
    max = Math.max(max, heads[j].clock[i])
  }
  return max
}

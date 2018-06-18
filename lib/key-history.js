var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var get = require('./get')
var normalizeKey = require('./normalize')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  nanoiterator.call(this)
  this._db = db
  this._prefix = normalizeKey(prefix)
  this._heads = undefined
}

inherits(Iterator, nanoiterator)

Iterator.prototype._open = function (cb) {
  this._db.heads((err, heads) => {
    if (err) return cb(err)
    this._heads = heads
    cb()
  })
}

Iterator.prototype._next = function (cb) {
  if (!this._heads || !this._heads.length) return cb(null, null)
  get(this._db, this._heads, this._prefix,
    { reduce: false, deletes: true },
    (err, nodes) => {
      if (err) return cb(err)
      if (nodes.length === 0) return cb(null, null)
      this._nextHeads(nodes, (err, heads) => {
        if (err) return cb(err)
        this._heads = heads
        cb(null, nodes)
      })
    })
}

Iterator.prototype._nextHeads = function (nodes, cb) {
  var i
  var heads = []
  var error = null
  var missing = 0

  for (i = 0; i < nodes.length; i++) {
    var node = nodes[i]
    for (var c = 0; c < node.clock.length; c++) {
      var seq = node.clock[c]
      if (c !== node.feed && seq > 2) {
        missing++
        this._db._writers[c].get(seq - 1, onHead)
      } else if (c === node.feed && node.seq > 1) {
        missing++
        this._db._writers[node.feed].get(node.seq - 1, onHead)
      }
    }
  }
  if (missing === 0) cb(null, undefined)

  function onHead (err, head) {
    if (head) heads.push(head)
    if (err) error = err
    if (--missing) return

    cb(error, filterHeads(heads))
  }
}

function filterHeads (list) {
  var heads = []
  for (var i = 0; i < list.length; i++) {
    if (isHead(list[i], list)) heads.push(list[i])
  }
  return heads
}

function isHead (node, list) {
  if (!node) return false
  var clock = node.seq + 1
  for (var i = 0; i < list.length; i++) {
    var other = list[i]
    if (other === node || !other) {
      continue
    }
    if ((other.clock[node.feed] || 0) >= clock) return false
  }
  return true
}

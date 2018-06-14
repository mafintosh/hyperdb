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
  const heads = []
  const numFeeds = this._db.feeds.length
  let error = null
  let missing = 0

  const feeds = []
  for (i = 0; i < nodes.length; i++) {
    const node = nodes[i]
    feeds.push(node.feed)
    if (node.seq <= 1) continue
    missing++
    this._db._writers[node.feed].get(node.seq - 1, onHead)
  }

  if (numFeeds !== nodes.length) {
    const seqs = mostRecent(nodes, feeds)
    for (i = 0; i < numFeeds; i++) {
      const seq = seqs[i]
      if (!seq || seq <= 1) continue
      missing++
      this._db._writers[i].get(seq, onHead)
    }
  }

  if (missing === 0) cb(null, undefined)
  function onHead (err, head) {
    if (head) heads.push(head)
    if (err) error = err
    if (--missing) return
    cb(error, heads)
  }
}

function mostRecent (nodes, feeds) {
  const seqs = []
  for (var n = 0; n < nodes.length; n++) {
    const trie = nodes[n].trie
    for (var i = 0; i < trie.length; i++) {
      const t = trie[i]
      for (var j = 0; j < t.length; j++) {
        const ptrs = t[j]
        if (!ptrs) continue
        for (var p = 0; p < ptrs.length; p++) {
          const ptr = ptrs[p]
          if (!ptr || feeds.includes(ptr.feed)) continue
          if (seqs[ptr.feed] === undefined || ptr.seq > seqs[ptr.feed]) seqs[ptr.feed] = ptr.seq
        }
      }
    }
  }
  return seqs
}

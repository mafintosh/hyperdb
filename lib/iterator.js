var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var options = require('./options')
var hash = require('./hash')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  nanoiterator.call(this)

  this._db = db
  this._workers = []
  this._recursive = opts.recursive !== false
  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)
  this._feeds = db._feeds
  this._prefix = prefix
  this._filter = null
  this._end = 0
  this._start = 0
  this._opened = false
  this._updated = false
}

inherits(Iterator, nanoiterator)

Iterator.prototype._open = function (cb) {
  var self = this

  // do a prefix search to find the heads for the prefix
  // we are trying to scan
  this._db._getNodes(this._prefix, {prefix: true}, onheads)

  function onheads (err, nodes) {
    if (err) return cb(err)

    var prefixLength = hash(self._prefix, false).length

    self._start = prefixLength
    self._end = prefixLength + (self._recursive ? Infinity : hash.LENGTH)

    for (var i = 0; i < nodes.length; i++) {
      self._createWorker().push(nodes[i], prefixLength)
    }

    cb(null)
  }
}

Iterator.prototype._createWorker = function () {
  var w = new Worker(this)
  this._workers.push(w)
  if (this._workers.length > 1) {
    this._filter = new Map()
    for (var i = 0; i < this._workers.length; i++) {
      this._workers[i]._filter()
    }
  }
  this._updated = true
  return w
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

Iterator.prototype._next = function (cb) {
  // length can mutate during iteration, so fix it
  var len = this._workers.length
  var missing = len
  var error = null
  var self = this

  if (!missing) return process.nextTick(cb, null, null)

  for (var i = 0; i < len; i++) {
    var w = this._workers[i]
    if (!w.value) w.next(done)
    else done(null)
  }

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)
    self._consume(cb)
  }
}

Iterator.prototype._consume = function (cb) {
  if (this._updated) {
    this._updated = false
    this._next(cb)
    return
  }

  if (this._workers.length === 1) {
    if (this._workers[0].ended) return cb(null, null)
    // TODO: fast case for single node
    cb(null, this._prereturn([this._workers[0].consume()]))
    return
  }

  var min = minKey(this._workers)
  var nodes = []

  for (var i = 0; i < this._workers.length; i++) {
    var w = this._workers[i]
    if (w.value && w.value.key === min.key) nodes.push(w.consume())
  }

  if (!nodes.length) return cb(null, null)
  cb(null, this._prereturn(nodes))
}

function Worker (ite) {
  this._iterator = ite
  this._db = ite._db
  this._stack = []
  this._error = null

  this.value = null
  this.ended = false
}

Worker.prototype.next = function (cb) {
  next(this, cb)
}

Worker.prototype.push = function (node, i) {
  this._stack.push({node, i, feed: node.feed, seq: node.seq})
}

Worker.prototype.pushAndGet = function (ptr, i) {
  if (!this._unique(ptr.feed, ptr.seq)) return

  var self = this
  var wrap = {node: null, i, feed: ptr.feed, seq: ptr.seq}

  this._stack.push(wrap)
  this._db._getPointer(ptr.feed, ptr.seq, onpointer)

  function onpointer (err, node) {
    self._onnode(wrap, err, node)
  }
}

Worker.prototype.consume = function () {
  var node = this.value
  if (!node) return null

  this.value = null
  if (this._iterator._filter) {
    this._iterator._filter.delete(id(node.feed, node.seq))
  }

  return node
}

Worker.prototype.pop = function () {
  var len = this._stack.length

  if (!len) {
    this.ended = true
    return null
  }

  if (this._stack[len - 1].node) {
    return this._stack.pop()
  }

  return null
}

Worker.prototype.wait = function (cb) {
  this._callback = cb
}

Worker.prototype._unique = function (feed, seq) {
  if (!this._iterator._filter) return true
  var key = id(feed, seq)
  if (this._iterator._filter.has(key)) return false
  this._iterator._filter.set(key, true)
  return true
}

Worker.prototype._filter = function () {
  var filter = false

  for (var i = 0; i < this._stack.length; i++) {
    var s = this._stack[i]
    var k = id(s.feed, s.seq)
    if (this._iterator._filter.has(k)) {
      this._stack[i] = null
      filter = true
      continue
    }
    this._iterator._filter.set(k, true)
  }

  if (!filter) return

  this._stack = this._stack.filter(notNull)

  if (!this._callback) return

  var cb = this._callback
  this._callback = null
  next(this, cb)
}

Worker.prototype._onnode = function (wrap, error, node) {
  if (error) this._error = error
  else wrap.node = node

  if (!this._callback || wrap !== this._stack[this._stack.length - 1]) {
    return
  }

  var cb = this._callback
  var err = this._error

  this._error = null
  this._callback = null

  if (err) return cb(err)
  next(this, cb)
}

function next (worker, cb) {
  // Do a BFS search of the trie based data structure
  // Results are sorted based on the path hash.
  // The stack nodes look like this, {i, node}. i is the start index of the
  // trie of the corresponding node.

  var top = worker.pop()

  if (!top) {
    if (worker.ended) return process.nextTick(cb, null)
    return worker.wait(cb)
  }

  var node = top.node
  var iterator = worker._iterator
  var end = Math.min(iterator._end, node.trie.length)

  // Look in the trie (starting at next.i) and see if there is a trie split.
  // If there is one, add them to the stack and recursively call next
  for (var i = top.i; i < end; i++) {
    var bucket = node.trie[i]
    if (!bucket || !bucket.length) continue

    // We have a trie split! Traverse the bucket in reverse order so the
    // nodes get added to the stack in hash sorted order. We start at j=4
    // cause 3 is the highest value of our 2 bit hash values
    // (0b11, 0b10, 0b01, 0b00) and 3 + 1 is the END_OF_HASH value.
    // If this is at the very beginning of the prefix *do not* include
    // hash terminations, otherwise foo/ is included in iterator(foo/).
    var start = iterator._start === i ? 3 : 4
    for (var j = start; j >= 0; j--) {
      // Include our node itself again but update i. This makes sure the
      // node is emitted in the right order
      if (j === node.path[i]) {
        worker.push(node, i + 1)
        continue
      }

      // Resolve the nodes referenced by the trie and add them to the stack

      var values = j < bucket.length && bucket[j]
      if (!values || !values.length) continue

      worker.pushAndGet(values[0], i + 1)
      // If values.length > 1, we have a fork.
      // Create some workers to handle that.
      for (var k = 1; k < values.length; k++) {
        iterator._createWorker().pushAndGet(values[k], i + 1)
      }
    }

    // Recursively call next again with the updated stack and return.
    // The stack is guaranteed to have been updated in this loop, so this
    // is always safe to do.
    return nextNT(worker, cb)
  }

  worker.value = node
  process.nextTick(cb, null)
}

function nextNT (worker, cb) {
  process.nextTick(next, worker, cb)
}

function notNull (s) {
  return s
}

function minKey (workers) {
  var min = null
  for (var i = 0; i < workers.length; i++) {
    var t = workers[i]
    if (!min || !min.value) min = t
    if (!t.value) continue
    if (sortKey(t.value).localeCompare(sortKey(min.value)) < 0) {
      min = t
    }
  }
  return min && min.value
}

function sortKey (node) {
  return node.path.join('') + '@' + node.key
}

function id (feed, seq) {
  return seq + '@' + feed
}

var hash = require('./hash')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  this._recursive = opts.recursive !== false
  this._map = opts.map || db._map
  this._reduce = opts.reduce || db._reduce
  this._feeds = db._feeds
  this._prefix = prefix
  this._threads = []
  this._end = 0
  this._opened = false
  this._updated = false
  this._db = db
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

Iterator.prototype._openAndNext = function (cb) {
  var self = this

  // do a prefix search to find the heads for the prefix
  // we are trying to scan
  this._db._getNodes(this._prefix, {prefix: true}, onheads)

  function onheads (err, nodes) {
    if (err) return cb(err)

    if (self._opened) return next(self, cb)
    self._opened = true

    if (nodes) {
      var prefixLength = hash(self._prefix, false).length
      self._end = prefixLength

      for (var i = 0; i < nodes.length; i++) {
        self._addThread().push(nodes[i], prefixLength)
      }
    }

    self._end += self._recursive ? Infinity : hash.LENGTH
    self.next(cb)
  }
}

Iterator.prototype._addThread = function () {
  this._updated = true
  var t = new Thread(this._threads)
  this._threads.push(t)
  return t
}

Iterator.prototype.next = function (cb) {
  if (!this._opened) return this._openAndNext(cb)

  this._updated = false

  var missing = 0
  var error = null
  var self = this

  for (var i = 0; i < this._threads.length; i++) {
    var t = this._threads[i]
    if (t.result || t.finished) continue
    missing++
    next(this, t, done)
  }

  if (!missing) {
    missing = 1
    done(null)
  }

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)

    if (self._updated) {
      return self.next(cb)
    }

    if (self._threads.length === 1) {
      if (self._threads[0].finished) return cb(null, null)
      var node = self._threads[0].result
      self._threads[0].result = null
      cb(null, self._prereturn([node])) // TODO: fast case for single node prereturn
      return
    }

    var min = minKey(self._threads)
    var nodes = []

    for (var i = 0; i < self._threads.length; i++) {
      var t = self._threads[i]
      if (t.result && t.result.key === min.key) {
        nodes.push(t.result)
        t.result = null
      }
    }

    if (!nodes.length) return cb(null, null)
    cb(null, self._prereturn(nodes))
  }
}

function minKey (threads) {
  var min = null
  for (var i = 0; i < threads.length; i++) {
    var t = threads[i]
    if (!min || !min.result) min = t
    if (!t.result) continue
    if (sortKey(t.result).localeCompare(sortKey(min.result)) < 0) {
      min = t
    }
  }
  return min && min.result
}

function sortKey (node) {
  return node.path.join('') + '@' + node.key
}

function next (self, thread, cb) {
  // Do a BFS search of the trie based data structure
  // Results are sorted based on the path hash.
  // The stack nodes look like this, {i, node}. i is the start index of the
  // trie of the corresponding node.

  // If nothing is on the stack, we are done (null signals end of iterator)
  var stack = thread.stack
  if (!stack.length) {
    thread.finished = true
    return process.nextTick(cb, null, thread)
  }

  var top = stack.pop()
  var node = top.node
  var end = Math.min(self._end, node.trie.length)

  // Look in the trie (starting at next.i) and see if there is a trie split.
  // If there is one, add them to the stack and recursively call next
  for (var i = top.i; i < end; i++) {
    var bucket = node.trie[i]
    if (!bucket || !bucket.length) continue

    // We have a trie split! Traverse the bucket in reverse order so the nodes
    // get added to the stack in hash sorted order. We start at j=3 cause 3 is
    // the highest value of our 2 bit hash values (0b11, 0b10, 0b01, 0b00).
    for (var j = 3; j >= 0; j--) {
      // Include our node itself again but update i. This makes sure the node
      // is emitted in the right order
      if (j === node.path[i]) {
        thread.push(node, i + 1)
        continue
      }

      // Resolve the nodes referenced by the trie and add them to the stack

      var values = j < bucket.length && bucket[j]
      if (!values || !values.length) continue

      if (values.length > 1) { // a fork is happening
        var all = values.map((val) => self._feeds[val.feed][val.seq])

        thread.push(all[0], i + 1)
        for (var k = 1; k < all.length; k++) {
          self._addThread().push(all[k], i + 1)
        }
      } else {
        var otherNode = self._feeds[values[0].feed][values[0].seq]
        thread.push(otherNode, i + 1)
      }
    }

    // Recursively call next again with the updated stack and return.
    // The stack is guaranteed to have been updated in this loop, so this
    // is always safe to do.
    process.nextTick(next, self, thread, cb)
    return
  }

  // We iterated the entire trie of the node.
  // Time to return it from the iterator
  thread.result = node
  process.nextTick(cb, null, thread)
}

function Thread (threads) {
  this.threads = threads
  this.stack = []
  this.result = null
  this.finished = false
}

Thread.prototype._unique = function (node) {
  if (this.threads.length < 2) return true
  for (var i = 0; i < this.threads.length; i++) {
    var t = this.threads[i]
    if (t === this) continue

    // This should be optimisable
    // TODO: track the perf cost of this
    if (t.result && same(t.result, node)) return false

    for (var j = 0; j < t.stack.length; j++) {
      if (same(t.stack[j].node, node)) return false
    }
  }
  return true
}

Thread.prototype.push = function (node, i) {
  if (!this._unique(node)) return
  this.stack.push({node: node, i: i})
}

function same (a, b) {
  return a.feed === b.feed && a.seq === b.seq
}

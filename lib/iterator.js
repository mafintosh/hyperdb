var hash = require('./hash')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  this._recursive = opts.recursive !== false
  this._nodes = db._nodes
  this._prefix = prefix
  this._stack = []
  this._end = 0
  this._opened = false
  this._db = db
}

Iterator.prototype._openAndNext = function (cb) {
  var self = this

  // do a prefix search to find the heads for the prefix
  // we are trying to scan
  this._db.get(this._prefix, {prefix: true}, onheads)

  function onheads (err, nodes) {
    if (err) return cb(err)

    if (self._opened) return next(self, cb)
    self._opened = true

    if (nodes) {
      var prefixLength = hash(self._prefix, false).length
      self._end = prefixLength
      if (nodes.length > 1) throw new Error('nope, too many')
      self._stack.push({i: prefixLength, node: nodes[0]})
    }

    self._end += self._recursive ? Infinity : hash.LENGTH
    next(self, cb)
  }
}

Iterator.prototype.next = function (cb) {
  if (!this._opened) return this._openAndNext(cb)
  next(this, cb)
}

function next (self, cb) {
  // Do a BFS search of the trie based data structure
  // Results are sorted based on the path hash.
  // The stack nodes look like this, {i, node}. i is the start index of the
  // trie of the corresponding node.

  // If nothing is on the stack, we are done (null signals end of iterator)
  if (!self._stack.length) return cb(null, null)

  var top = self._stack.pop()
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
        self._stack.push({i: i + 1, node: node})
        continue
      }

      // Resolve the nodes referenced by the trie and add them to the stack

      var values = j < bucket.length && bucket[j]
      if (!values || !values.length) continue
      if (values.length > 1) throw new Error('nope, too many')

      var other = self._nodes[values[0].seq]
      self._stack.push({i: i + 1, node: other})
    }

    // Recursively call next again with the updated stack and return.
    // The stack is guaranteed to have been updated in this loop, so this
    // is always safe to do.
    process.nextTick(next, self, cb)
    return
  }

  // We iterated the entire trie of the node.
  // Time to return it from the iterator
  cb(null, [node])
}

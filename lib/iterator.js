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
  this._recursive = opts.recursive !== false
  this._prefix = prefix
  this._workers = []
  this._end = 0
  this._start = 0
  this._updated = false
  this._gt = !!opts.gt
  this._sortOrder = opts.latest ? 'latest' : 'hash'
  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)
}

inherits(Iterator, nanoiterator)

Iterator.prototype._open = function (cb) {
  var self = this

  // do a prefix search to find the heads for the prefix
  // we are trying to scan
  var opts = {prefix: true, map: false, reduce: false}
  this._db.get(this._prefix, opts, function (err, heads) {
    if (err) return cb(err)

    var prefixLength = hash(self._prefix, false).length

    self._start = prefixLength
    self._end = prefixLength + (self._recursive ? Infinity : hash.LENGTH)

    self._fork(heads, null, self._start)
    cb()
  })
}

Iterator.prototype._next = function (cb) {
  var len = this._workers.length
  var missing = len + 1
  var error = null
  var self = this

  for (var i = 0; i < len; i++) {
    var w = this._workers[i]
    if (w && !w.value && !w.ended) {
      next(this, w, done)
    } else {
      missing--
    }
  }

  done(null)

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)
    self._consume(cb)
  }
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

Iterator.prototype._consume = function (cb) {
  if (this._updated) {
    this._updated = false
    this._next(cb)
    return
  }

  if (this._workers.length === 1) {
    if (this._workers[0].ended) return cb(null, null)
    var node = consume(this._workers[0])
    if (node.value === null) return this._next(cb)
    // TODO: fast case for single node
    cb(null, this._prereturn([node]))
    return
  }

  var min = minKey(this._workers, this._sortOrder)
  var nodes = []

  for (var i = 0; i < this._workers.length; i++) {
    var w = this._workers[i]
    if (w.value && w.value.key === min.key) nodes.push(consume(w))
  }

  if (!nodes.length) return cb(null, null)
  if (allDeletes(nodes)) return this._next(cb)
  cb(null, this._prereturn(nodes))
}

Iterator.prototype._fork = function (nodes, from, offset) {
  if (!nodes.length) return

  if (from) {
    from.forks = nodes.length
    var idx = this._workers.indexOf(from)
    if (idx > -1) this._workers.splice(idx, 1)
  }

  for (var i = 0; i < nodes.length; i++) {
    var w = new Worker(nodes[i], offset, from)
    this._workers.push(w)
    this._updated = true
    if (!this._isHead(w.lock, w)) this._endWorker(w, true)
  }
}

Iterator.prototype._endWorker = function (worker) {
  if (worker.from) this._endFork(worker)

  if (worker.notHead) {
    var i = this._workers.indexOf(worker)
    this._workers.splice(i, 1)
    this._updated = true
  }
}

Iterator.prototype._endFork = function (worker) {
  if (--worker.from.forks) return
  this._workers.push(worker.from)
  this._updated = true

  var updated = true
  while (updated) {
    updated = false
    for (var i = 0; i < this._workers.length; i++) {
      if (this._workers[i].from === worker.from) {
        this._workers.splice(i, 1)
        updated = true
        break
      }
    }
  }
}

Iterator.prototype._isHead = function (head, worker) {
  var clock = head.seq + 1

  for (var i = 0; i < this._workers.length; i++) {
    var otherWorker = this._workers[i]
    if (otherWorker === worker) continue

    var otherClock = otherWorker.lock.clock[head.feed]
    if (clock <= otherClock) return false
  }

  return true
}

function Worker (head, i, from) {
  this.lock = head
  this.value = null
  this.forks = 0
  this.ended = false
  this.stack = []
  this.pending = 0
  this.error = null
  this.callback = null
  this.from = from

  this.stack.push({node: head, i, fork: null})
}

function copyLock (lock) {
  return {
    clock: lock.clock.slice(0)
  }
}

function consume (worker) {
  var val = worker.value
  worker.value = null
  return val
}

function wait (worker, cb) {
  worker.callback = cb
}

function fork (iterator, worker, nodes, i, cb) {
  iterator._fork(nodes, worker, i)
  cb(null)
}

function allSkips (stack, iterator, worker) {
  return stack.every(t => !iterator._isHead(t.node, worker))
}

function sortNodesByClock (a, b) {
  var isGreater = false
  var isLess = false
  var length = a.clock.length
  if (b.clock.length > length) length = b.clock.length
  for (var i = 0; i < length; i++) {
    var diff = (a.clock[i] || 0) - (b.clock[i] || 0)
    if (diff > 0) isGreater = true
    if (diff < 0) isLess = true
  }
  if (isGreater && isLess) return 0
  if (isLess) return -1
  if (isGreater) return 1
  return 0
}

function sortStackByClockAndSeq (a, b) {
  a = a.node || a.value
  b = b.node || b.value
  if (a && !b) return 1
  if (b && !a) return 1
  var sortValue = sortNodesByClock(a, b)
  if (sortValue !== 0) return sortValue
  // // same time, so use sequence to order
  if (a.feed === b.feed) return a.seq - b.seq
  var bOffset = b.clock.reduce((p, v) => p + v, b.seq)
  var aOffset = a.clock.reduce((p, v) => p + v, a.seq)
  // if real sequence is the same then return sort on feed
  if (bOffset === aOffset) return b.feed - a.feed
  return aOffset - bOffset
}

function pop (iterator, worker) {
  while (true) {
    var len = worker.stack.length
    if (len) {
      if (iterator._sortOrder === 'latest') worker.stack.sort(sortStackByClockAndSeq)
      var top = worker.stack.pop()
      if (top.node && !iterator._isHead(top.node, worker)) {
        if (!allSkips(worker.stack, iterator, worker)) {
          worker.lock = copyLock(worker.lock)

          // Unsure if the below logic is 100% the right this to do
          // but the test pass, so ...
          // var max = 0
          // TODO: commenting this out fixes a weird issue with some
          // nodes not getting iterated. Once we have more tests, this should just get removed
          // for (var i = 0; i < worker.stack.length; i++) {
          //   var node = worker.stack[i].node
          //   if (node.feed !== top.node.feed || !iterator._isHead(node, worker)) continue
          //   max = Math.max(node.clock[top.node.feed], max)
          // }
          worker.lock.clock[top.node.feed] = 0
        } else {
          worker.notHead = true
        }
        continue
      }
      return top
    }
    worker.ended = true
    return null
  }
}

function pushPointer (iterator, worker, ptr, i) {
  var index = worker.stack.push({i, node: ptr, fork: null}) - 1
  worker.pending++
  iterator._db._getPointer(ptr.feed, ptr.seq, false, done)

  function done (err, node) {
    if (err) worker.error = err
    else worker.stack[index].node = node
    if (--worker.pending) return
    if (worker.error) return worker.callback(err)
    next(iterator, worker, worker.callback)
  }
}

function pushFork (iterator, worker, ptrs, i) {
  var index = worker.stack.push({i, node: null, fork: null}) - 1
  worker.pending++
  iterator._db._getAllPointers(ptrs, false, done)

  function done (err, nodes) {
    if (err) worker.error = err
    else worker.stack[index].fork = nodes
    if (--worker.pending) return
    if (worker.error) return worker.callback(err)
    next(iterator, worker, worker.callback)
  }
}

function next (iterator, worker, cb) {
  // Do a BFS search of the trie based data structure
  // Results are sorted based on the path hash.
  // The stack nodes look like this, {i, node}. i is the start index of the
  // trie of the corresponding node.

  var top = pop(iterator, worker)

  // If nothing was on the stack, it's either because the worker has ended
  // and we should stop.

  if (!top) {
    iterator._endWorker(worker)
    return process.nextTick(cb, null)
  }

  if (top.fork) return fork(iterator, worker, top.fork, top.i, cb)

  var node = top.node
  var end = Math.min(iterator._end, node.trie.length)

  for (var i = top.i; i < end; i++) {
    var bucket = node.trie[i]
    if (!bucket || !bucket.length) continue

    // We have a trie split! Traverse the bucket in reverse order so the
    // nodes get added to the stack in hash sorted order (4, 0, 1, 2, 3).
    // We start at 4 cause we want 'a' to come before 'a/b', and 4 is the
    // hash termination value.

    // If this is at the very beginning of the prefix AND the iterator
    // is a gt (gte is default) then *do not* include hash terminations.
    // If we are dealing with the empty prefix, then always do gt as we store
    // auth messages under '' and never wanna include them here
    var gt = iterator._gt || !iterator._start
    var sortEnd = gt && iterator._start === i ? 4 : 5

    for (var j = 0; j < sortEnd; j++) {
      var sortValue = j === 4 ? 4 : 3 - j // 3, 2, 1, 0, 4

      // Include our node itself again but update i. This makes sure the
      // node is emitted in the right order
      if (sortValue === node.path[i]) {
        worker.stack.push({i: i + 1, node, fork: null})
        if (sortValue !== 4) continue
      }

      // Resolve the nodes referenced by the trie and add them to the stack
      // If there are more than a single value in the trie split we need to
      // add new workers to resolve those forks.

      var values = sortValue < bucket.length && bucket[sortValue]
      if (!values || !values.length) continue

      if (values.length === 1) pushPointer(iterator, worker, values[0], i + 1)
      else pushFork(iterator, worker, values, i + 1)
    }

    if (worker.pending) return wait(worker, cb)

    // Recursively call next again with the updated stack and return.
    // The stack is guaranteed to have been updated in this loop, so this
    // is always safe to do.
    return nextNT(iterator, worker, cb)
  }

  worker.value = node
  process.nextTick(cb, null)
}

function nextNT (iterator, worker, cb) {
  process.nextTick(next, iterator, worker, cb)
}

function hashSort (a, b) {
  return sortKey(b.value).localeCompare(sortKey(a.value))
}

function minKey (workers, sortOrder) {
  var min = null
  var sortFn = sortOrder === 'hash' ? hashSort : sortStackByClockAndSeq
  for (var i = 0; i < workers.length; i++) {
    var t = workers[i]
    if (!min || !min.value) min = t
    if (!t.value) continue
    if (sortFn(t, min) >= 0) {
      min = t
    }
  }
  return min && min.value
}

function sortKey (node) {
  return node.path.slice(0, -1).join('') + '@' + node.key
}

function allDeletes (list) {
  for (var i = 0; i < list.length; i++) {
    if (list[i].value !== null) return false
  }
  return true
}

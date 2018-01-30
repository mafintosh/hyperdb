var hash = require('./hash')
var options = require('./options')

module.exports = get

function get (db, heads, key, opts, cb) {
  if (typeof opts === 'function') return get(db, heads, key, null, opts)
  if (!heads.length) return process.nextTick(cb, null, [])

  var req = new GetRequest(db, key, opts)
  req.callback = cb
  req.update(heads, null)
}

function GetRequest (db, key, opts) {
  this.key = key
  this.results = []
  this.callback = noop

  this._options = opts || null
  this._prefixed = !!(opts && opts.prefix)
  this._path = hash(key, !this._prefixed)
  this._db = db
  this._locks = new Array(db._feeds.length)
  this._error = null
  this._active = 0
}

GetRequest.prototype.push = function (node) {
  if (this._prefixed && isPrefix(node.key, this.key)) {
    this.results.push(node)
  } else if (node.key === this.key) {
    this.results.push(node)
  }
}

GetRequest.prototype.update = function (nodes, worker) {
  for (var feedId = 0; feedId < this._locks.length; feedId++) {
    if (worker && worker.lock !== this._locks[feedId]) continue
    this._locks[feedId] = getHighestClock(nodes, feedId)
  }

  this._active += nodes.length

  for (var i = 0; i < nodes.length; i++) {
    var next = new Worker(nodes[i], worker ? worker.i + 1 : 0)
    if (this._hasLock(next, next.head)) this._moveCloser(next)
    else this._end(next, null)
  }

  if (worker) this._end(worker, null)
}

GetRequest.prototype._end = function (worker, err) {
  if (err) this._error = err
  if (--this._active) return

  var error = this._error
  var cb = this.callback

  this._error = this.callback = null

  if (error) cb(error)
  else cb(null, this._prereturn(this.results))
}

GetRequest.prototype._prereturn = function (results) {
  var map = options.map(this._options, this._db)
  var reduce = options.reduce(this._options, this._db)
  if (map) results = results.map(map)
  if (reduce) return results.length ? results.reduce(reduce) : null
  return results
}

GetRequest.prototype._updatePointers = function (ptrs, worker) {
  var self = this

  this._db._getAllPointers(ptrs, onnodes)

  function onnodes (err, nodes) {
    if (err) return self._end(err, worker)
    self.update(nodes, worker)
  }
}

GetRequest.prototype._getAndMoveCloser = function (ptr, worker) {
  var self = this
  this._db._getPointer(ptr.feed, ptr.seq, onnode)

  function onnode (err, node) {
    if (err) return self._end(worker, err)
    if (!self._hasLock(worker, node)) return self._end(worker, null)

    worker.head = node
    worker.i++
    self._moveCloser(worker)
  }
}

GetRequest.prototype._pushPointers = function (ptrs, worker) {
  var self = this

  this._db._getAllPointers(ptrs, onresults)

  function onresults (err, nodes) {
    if (err) return self._end(err, null)

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i]
      if (self._hasLock(worker, node)) self.push(node)
    }

    self._end(worker, null)
  }
}

GetRequest.prototype._moveCloser = function (worker) {
  var path = this._path
  var head = worker.head

  // If no head -> 404
  if (!head) return this._end(worker, null)

  // We want to find the key closest to our path.
  // At max, we need to go through path.length iterations
  for (; worker.i < path.length; worker.i++) {
    var i = worker.i
    var val = path[i]
    if (head.path[i] === val) continue

    // We need a closer node. See if the trie has one that
    // matches the path value
    var remoteBucket = head.trie[i] || []
    var remoteValues = remoteBucket[val] || []

    // No closer ones -> 404
    if (!remoteValues.length) return this._end(worker, null)

    // More than one reference -> We have forks.
    if (remoteValues.length > 1) this._updatePointers(remoteValues, worker)
    else this._getAndMoveCloser(remoteValues[0], worker)
    return
  }

  this.push(head)

  // TODO: not sure if this is even needed!
  // check if we had a collision, or similar
  // (our last bucket contains more stuff)

  var top = path.length - 1
  var last = head.trie[top]
  var lastValues = last && last[path[top]]
  if (!lastValues || !lastValues.length) return this._end(worker, null)

  this._pushPointers(lastValues, worker)
}

GetRequest.prototype._hasLock = function (worker, node) {
  return this._locks[node.feed] === worker.lock
}

function Worker (head, i) {
  this.i = i
  this.head = head
  this.lock = head
}

function noop () {}

function getHighestClock (nodes, feedId) {
  var highest = null

  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i]

    if (!highest) {
      highest = node
      continue
    }

    var hclock = highest.clock
    var nclock = node.clock

    if (nclock.length <= feedId) continue
    if (hclock.length <= feedId || nclock[feedId] > hclock[feedId]) {
      highest = node
    }
  }

  return highest
}

function isPrefix (key, prefix) {
  if (prefix.length && prefix[0] === '/') prefix = prefix.slice(1)
  return key.slice(0, prefix.length) === prefix
}

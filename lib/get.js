var hash = require('./hash')
var options = require('./options')

module.exports = get

function get (db, heads, key, opts, cb) {
  if (typeof opts === 'function') return get(db, heads, key, null, opts)

  var req = new GetRequest(db, key, opts)
  req.start(heads, cb)
}

function GetRequest (db, key, opts) {
  this.key = key
  this.results = []

  this._callback = noop
  this._options = opts || null
  this._prefixed = !!(opts && opts.prefix)
  this._path = (opts && opts.path) || hash(key, !this._prefixed)
  this._onlookup = (opts && opts.onlookup) || null
  this._db = db
  this._error = null
  this._active = 0
  this._workers = []
}

GetRequest.prototype._push = function (node) {
  if (this._prefixed) {
    this.results.push(node)
  } else if (node.key === this.key) {
    this.results.push(node)
  }
}

GetRequest.prototype.start = function (heads, cb) {
  if (cb) this._callback = cb
  if (!heads.length) return process.nextTick(finalize, this)

  if (this._onlookup) {
    for (var i = 0; i < heads.length; i++) {
      this._onlookup({feed: heads[i].feed, seq: heads[i].seq})
    }
  }

  this._update(heads, null)
}

GetRequest.prototype._update = function (nodes, worker) {
  if (worker) {
    var r = this._workers.indexOf(worker)
    if (r > -1) this._workers.splice(r, 1)
  }

  this._active += nodes.length

  for (var i = 0; i < nodes.length; i++) {
    var next = new Worker(nodes[i], worker ? worker.i + 1 : 0)
    this._workers.push(next)
    if (this._isHead(next.lock, next)) this._moveCloser(next)
    else this._end(next, null, true)
  }

  if (worker) {
    this._end(worker, null)
  }
}

GetRequest.prototype._end = function (worker, err, removeWorker) {
  if (removeWorker) {
    var i = this._workers.indexOf(worker)
    if (i > -1) this._workers.splice(i, 1)
  }

  if (err) this._error = err
  if (--this._active) return
  this._finalize()
}

GetRequest.prototype._finalize = function () {
  var error = this._error
  var cb = this._callback

  this._error = this._callback = null

  if (error) cb(error)
  else cb(null, this._prereturn(this.results))
}

GetRequest.prototype._prereturn = function (results) {
  // TODO: the extra prefixed check should prob be it's own option, ie deletes: true
  if (allDeletes(results) && !this._prefixed) results = []

  var map = options.map(this._options, this._db)
  var reduce = options.reduce(this._options, this._db)
  if (map) results = results.map(map)
  if (reduce) return results.length ? results.reduce(reduce) : null

  return results
}

GetRequest.prototype._updatePointers = function (ptrs, worker) {
  var self = this

  if (this._onlookup) mapPointers(this._onlookup, ptrs)
  this._db._getAllPointers(ptrs, false, onnodes)

  function onnodes (err, nodes) {
    if (err) return self._end(worker, err, false)
    self._update(nodes, worker)
  }
}

GetRequest.prototype._getAndMoveCloser = function (ptr, worker) {
  var self = this

  // TODO: make this optimisation *everywhere* (ie isHead(ptr) vs isHead(node))
  // if (!self._isHead(ptr, worker)) return self._end(worker, null)
  if (this._onlookup) this._onlookup(ptr)
  this._db._getPointer(ptr.feed, ptr.seq, false, onnode)

  function onnode (err, node) {
    if (err) return self._end(worker, err, false)

    if (!self._isHead(node, worker)) return self._end(worker, null, true)

    worker.head = node
    worker.i++
    self._moveCloser(worker)
  }
}

GetRequest.prototype._pushPointers = function (ptrs, worker) {
  var self = this

  if (this._onlookup) mapPointers(this._onlookup, ptrs)
  this._db._getAllPointers(ptrs, false, onresults)

  function onresults (err, nodes) {
    if (err) return self._end(worker, err, false)

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i]
      if (self._isHead(node, worker)) self._push(node)
    }

    self._end(worker, null, false)
  }
}

GetRequest.prototype._moveCloser = function (worker) {
  var path = this._path
  var head = worker.head

  // If no head -> 404
  if (!head) return this._end(worker, null, false)

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
    if (!remoteValues.length) return this._end(worker, null, false)

    // More than one reference -> We have forks.
    if (remoteValues.length > 1) this._updatePointers(remoteValues, worker)
    else this._getAndMoveCloser(remoteValues[0], worker)
    return
  }

  this._push(head)

  // TODO: not sure if this is even needed!
  // check if we had a collision, or similar
  // (our last bucket contains more stuff)

  var top = path.length - 1
  var last = head.trie[top]
  var lastValues = last && last[path[top]]
  if (!lastValues || !lastValues.length) return this._end(worker, null, false)

  this._pushPointers(lastValues, worker)
}

GetRequest.prototype._isHead = function (head, worker) {
  var clock = head.seq + 1

  for (var i = 0; i < this._workers.length; i++) {
    var otherWorker = this._workers[i]
    if (otherWorker === worker) continue

    var otherClock = otherWorker.lock.clock[head.feed]
    if (clock <= otherClock) return false
  }

  return true
}

function Worker (head, i) {
  this.i = i
  this.head = head
  this.lock = head
}

function noop () {}

function allDeletes (list) {
  for (var i = 0; i < list.length; i++) {
    if (!list[i].deleted) return false
  }
  return true
}

function finalize (req) {
  req._finalize()
}

function mapPointers (fn, ptrs) {
  for (var i = 0; i < ptrs.length; i++) fn(ptrs[i])
}

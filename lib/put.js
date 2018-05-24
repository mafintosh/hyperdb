var hash = require('./hash')

module.exports = put

function put (db, clock, heads, key, value, opts, cb) {
  if (typeof opts === 'function') return put(db, clock, heads, key, value, null, opts)
  var req = new PutRequest(db, key, value, clock, opts)
  req.start(heads, cb)
}

function PutRequest (db, key, value, clock, opts) {
  this.key = key
  this.value = value
  this.delete = !!(opts && opts.delete)

  this._clock = clock
  this._active = 0
  this._error = null
  this._callback = noop
  this._db = db
  this._path = hash(key, true)
  this._trie = []
}

PutRequest.prototype.start = function (heads, cb) {
  if (cb) this._callback = cb
  if (!heads.length) return this._finalize()
  this._update(heads, 0)
}

PutRequest.prototype._finalize = function () {
  var cb = this._callback
  var err = this._error

  this._error = this._callback = null

  if (err) return cb(err)

  // TODO: would be a cleaner api if we didn't require the clock to be passed in
  // but instead inferred it from the heads. Investigate...

  var node = {
    key: this.key,
    value: this.value,
    trie: this._trie,
    clock: this._clock
  }

  if (this.delete) node.deleted = true

  this._db._localWriter.append(node, function (err) {
    if (err) return cb(err)
    cb(null, node)
  })
}

PutRequest.prototype._update = function (heads, offset) {
  this._active += heads.length
  for (var i = 0; i < heads.length; i++) {
    var worker = new Worker(heads[i], offset)
    this._moveCloser(worker)
  }
}

PutRequest.prototype._updateHead = function (worker, feed, seq) {
  var self = this

  worker.pending++
  this._db._getPointer(feed, seq, true, function (err, node) {
    if (!err) worker.head = node
    self._workerDone(worker, err)
  })
}

PutRequest.prototype._workerDone = function (worker, err) {
  if (err) worker.error = err
  if (--worker.pending) return

  if (worker.error || worker.ended) {
    this._end(worker, worker.error)
  } else {
    worker.i++
    this._moveCloser(worker)
  }
}

PutRequest.prototype._fork = function (worker, ptrs) {
  var self = this

  worker.pending++
  this._db._getAllPointers(ptrs, true, function (err, nodes) {
    if (err) return self._workerDone(worker, err)
    self._update(nodes, worker.i + 1)
    self._workerDone(worker, null)
  })
}

PutRequest.prototype._checkCollision = function (worker, i, feed, seq) {
  var self = this

  worker.pending++
  this._db._getPointer(feed, seq, true, function (err, node) {
    if (err) return self._workerDone(worker, err)
    if (node.key !== self.key) self._push(worker, i, feed, seq)
    self._workerDone(worker, null)
  })
}

PutRequest.prototype._copyTrie = function (worker, bucket, val) {
  for (var i = 0; i < bucket.length; i++) {
    // check if we are the closest node, if so skip this
    // except if we are terminating the val. if so we
    // need to check for collions before making the decision
    if (i === val && val !== 4) continue

    var ptrs = bucket[i] || []
    for (var k = 0; k < ptrs.length; k++) {
      var ptr = ptrs[k]
      // if termination value, push if get(ptr).key !== key
      if (val === 4) this._checkCollision(worker, i, ptr.feed, ptr.seq)
      else this._push(worker, i, ptr.feed, ptr.seq)
    }
  }
}

PutRequest.prototype._splitTrie = function (worker, bucket, val) {
  var head = worker.head
  var headVal = head.path[worker.i]

  // check if we need to split the trie at all
  // i.e. is head still closest and is head not a conflict
  if (headVal === val && (headVal < 4 || head.key === this.key)) return

  // push head to the trie
  this._push(worker, headVal, head.feed, head.seq)

  var ptrs = bucket[val]

  if (!ptrs || !ptrs.length) {
    worker.ended = true
    return
  }

  this._updateHead(worker, ptrs[0].feed, ptrs[0].seq)
  if (ptrs.length > 1) this._fork(worker, ptrs.slice(1))
}

PutRequest.prototype._moveCloser = function (worker) {
  var path = this._path
  var head = worker.head

  for (; worker.i < path.length; worker.i++) {
    var i = worker.i
    var val = path[i]
    var bucket = head.trie[i] || []

    this._copyTrie(worker, bucket, val)
    this._splitTrie(worker, bucket, val)

    if (worker.pending) return
    if (worker.ended) break
  }

  this._end(worker, worker.error)
}

PutRequest.prototype._end = function (worker, err) {
  if (err) this._error = err
  if (!--this._active) this._finalize()
}

PutRequest.prototype._push = function (worker, val, feed, seq) {
  var i = worker.i
  var bucket = this._trie[i]
  if (!bucket) bucket = this._trie[i] = []
  var values = bucket[val]
  if (!values) bucket[val] = values = []

  for (var j = 0; j < values.length; j++) {
    var ref = values[j]
    if (ref.feed === feed && ref.seq === seq) return
  }

  values.push({feed, seq})
}

function Worker (head, i) {
  this.i = i
  this.head = head
  this.lock = head
  this.pending = 0
  this.error = null
  this.ended = false
}

function noop () {}

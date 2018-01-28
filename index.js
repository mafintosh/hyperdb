var toStream = require('nanoiterator/to-stream')
var hash = require('./lib/hash')
var iterator = require('./lib/iterator')
var differ = require('./lib/differ')
var changes = require('./lib/changes')

module.exports = DB

function DB (opts) {
  if (!(this instanceof DB)) return new DB(opts)
  if (!opts) opts = {}

  this._id = opts.id || 0
  this._feeds = []
  this._feeds[this._id] = []
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._snapshot = false
}

DB.prototype.snapshot = function () {
  var snapshot = new DB({id: this._id})
  snapshot._feeds = this._feeds.map(f => f.slice(0))
  snapshot._snapshot = true
  return snapshot
}

DB.prototype.heads = function () {
  var heads = []
  var i, j

  for (i = 0; i < this._feeds.length; i++) {
    if (this._feeds[i] && this._feeds[i].length) {
      heads.push(this._feeds[i][this._feeds[i].length - 1])
    }
  }

  // TODO: this could prob be done in O(heads) instead of O(heads^2)
  for (i = 0; i < heads.length; i++) {
    var h = heads[i]
    if (!h) continue

    for (j = 0; j < h.clock.length; j++) {
      var c = h.clock[j]
      if (heads[j] && heads[j].seq < c && j !== h.feed) {
        heads[j] = null
      }
    }
  }

  return heads.filter(x => x)
}

DB.prototype.put = function (key, val, cb) {
  if (!cb) cb = noop
  if (this._snapshot) {
    return process.nextTick(cb, new Error('Cannot put on a snapshot'))
  }

  key = normalizeKey(key)
  var path = hash(key, true)
  var writable = this._feeds[this._id]
  var clock = []

  for (var i = 0; i < this._feeds.length; i++) {
    clock[i] = (this._feeds[i] || []).length
    if (i === this._id) clock[i]++
  }

  if (!writable) writable = this._feeds[this._id] = []

  var heads = this.heads()
  var node = {
    path: path,
    feed: this._id,
    seq: writable.length,
    clock: clock,
    key: key,
    value: val,
    trie: [],
    [require('util').inspect.custom]: inspect
  }

  if (!heads.length) {
    writable.push(node)
    return process.nextTick(cb, null)
  }

  for (var j = 0; j < heads.length; j++) {
    this._put(node, 0, heads[j])
  }

  writable.push(node)
  process.nextTick(cb, null)
}

function inspect () {
  return `Node(key=${this.key}, value=${this.value}, seq=${this.seq}, feed=${this.feed})`
}

DB.prototype._put = function (node, i, head) {
  // TODO: when there is a fork, this will visit the same nodes more than once
  // which isn't a big deal, but unneeded - can be optimised away in the future

  // each bucket works as a bitfield
  // i.e. an index corresponds to a key (2 bit value) + 0b100 (hash.TERMINATE)
  // since this is eventual consistent + hash collisions there can be more than
  // one value for each key so this is a two dimensional array

  var localBucket
  var localValues
  var remoteBucket
  var remoteValues

  for (; i < node.path.length; i++) {
    var val = node.path[i] // the two bit value
    var headVal = head.path[i] // the two value of the current head

    localBucket = node.trie[i] // forks in the trie for this index
    remoteBucket = head.trie[i] || [] // remote forks

    // copy old trie for unrelated values
    for (var j = 0; j < remoteBucket.length; j++) {
      // if j === val, we are the newest node for this value
      // and we then don't want to copy the old trie.
      // if the value is a termination, we have a hash collision and then
      // we must copy it
      if (j === val && val !== hash.TERMINATE) continue

      if (!localBucket) localBucket = node.trie[i] = []
      if (!localBucket[j]) localBucket[j] = []
      localValues = localBucket[j]
      remoteValues = remoteBucket[j] || []

      for (var k = 0; k < remoteValues.length; k++) {
        var remoteVal = remoteValues[k]

        // might be a collions, check key and stuff
        if (val === hash.TERMINATE) {
          var resolved = this._feeds[remoteVal.feed][remoteVal.seq]
          // if it's the same key it's not a collision but an overwrite...
          if (resolved.key === node.key) continue
          // hash collision! fall through the if and add this value
        }

        // push the old value
        pushNoDups(localValues, remoteVal)
      }
    }

    // check if trie is splitting (either diff value or hash collision)
    if (headVal !== val || (headVal === hash.TERMINATE && head.key !== node.key)) {
      // we cannot follow the heads trie anymore --> change head to a closer one if possible

      // add head to our trie, so we reference back
      if (!localBucket) localBucket = node.trie[i] = []
      if (!localBucket[headVal]) localBucket[headVal] = []
      localValues = localBucket[headVal]

      pushNoDups(localValues, {feed: head.feed, seq: head.seq})

      // check if head has a closer pointer
      remoteValues = remoteBucket[val]
      if (!remoteValues || !remoteValues.length) break

      if (remoteValues.length > 1) { // more than one - fork out
        for (var l = 0; l < remoteValues.length; l++) {
          this._put(node, i + 1, this._feeds[remoteValues[l].feed][remoteValues[l].seq])
        }
        return
      }

      head = this._feeds[remoteValues[0].feed][remoteValues[0].seq]
      continue
    }
  }
}

function pushNoDups (list, val) {
  for (var i = 0; i < list.length; i++) {
    var l = list[i]
    if (l.feed === val.feed && l.seq === val.seq) return
  }
  list.push(val)
}

DB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)

  var self = this
  this._getNodes(key, opts, onnodes)

  function onnodes (err, results) {
    if (err) return cb(err)
    if (self._map) results = results.map(self._map)
    if (self._reduce) results = results.length ? results.reduce(self._reduce) : null
    cb(null, results)
  }
}

DB.prototype._getNodes = function (key, opts, cb) {
  key = normalizeKey(key)

  var results = []
  var heads = this.heads()

  var locks = getLocks(heads, this._feeds.length)

  for (var i = 0; i < heads.length; i++) {
    this._getNodesFromHead(key, opts, 0, heads[i], results, heads[i], locks)
  }

  process.nextTick(cb, null, results)
}

DB.prototype._getForks = function (key, opts, i, ptrs, results, lock, locks) {
  var nodes = getAllPtrs(this, ptrs)

  for (var feedId = 0; feedId < locks.length; feedId++) {
    var otherLock = locks[feedId]
    if (otherLock !== lock) continue
    locks[feedId] = getHighestClock(nodes, feedId)
  }

  for (var j = 0; j < nodes.length; j++) {
    this._getNodesFromHead(key, opts, i + 1, nodes[j], results, nodes[j], locks)
  }
}

DB.prototype._getNodesFromHead = function (key, opts, i, head, results, lock, locks) {
  var prefixed = !!(opts && opts.prefix)

  // If no head -> 404
  if (!head) return

  // Do not terminate the hash if it is a prefix search
  var path = hash(key, !prefixed)

  // We want to find the key closest to our path.
  // At max, we need to go through path.length iterations
  for (; i < path.length; i++) {
    var val = path[i]
    if (head.path[i] === val) continue

    // We need a closer node. See if the trie has one that
    // matches the path value
    var remoteBucket = head.trie[i] || []
    var remoteValues = remoteBucket[val] || []

    // No closer ones -> 404
    if (!remoteValues.length) return

    // More than one reference -> We have forks.
    if (remoteValues.length > 1) {
      this._getForks(key, opts, i, remoteValues, results, lock, locks)
      return
    }

    // Recursive from a closer node
    head = getPtr(this, remoteValues[0])
    if (locks[head.feed] !== lock) return
  }

  pushResult(prefixed, results, key, head)

  // check if we had a collision, or similar (our last bucket contains more stuff)

  var last = head.trie[path.length - 1]
  var lastValues = last && last[path[path.length - 1]]
  if (!lastValues) return

  for (var j = 0; j < lastValues.length; j++) {
    var lastVal = getPtr(this, lastValues[j])
    if (locks[val.feed] !== lock) continue
    pushResult(prefixed, results, key, lastVal)
  }
}

DB.prototype._getPointer = function (feed, seq, cb) {
  process.nextTick(cb, null, this._feeds[feed][seq])
}

DB.prototype.list = function (prefix, opts, cb) {
  if (typeof prefix === 'function') return this.list('', null, prefix)
  if (typeof opts === 'function') return this.list(prefix, null, opts)

  var ite = this.iterator(prefix, opts)
  var list = []

  ite.next(loop)

  function loop (err, nodes) {
    if (err) return cb(err)
    if (!nodes) return cb(null, list)
    list.push(nodes)
    ite.next(loop)
  }
}

DB.prototype.changes = function () {
  return changes(this)
}

DB.prototype.diff = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.diff(other, null, prefix)
  return differ(this, other || checkoutEmpty(this), prefix || '', opts)
}

function checkoutEmpty (db) {
  db = db.snapshot()
  db._feeds = []
  return db
}

DB.prototype.iterator = function (prefix, opts) {
  if (isOptions(prefix)) return this.iterator('', prefix)
  return iterator(this, normalizeKey(prefix || ''), opts)
}

DB.prototype.createChangesStream = function () {
  return toStream(this.changes())
}

DB.prototype.createDiffStream = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.createDiffStream(other, '', prefix)
  return toStream(this.diff(other, prefix, opts))
}

DB.prototype.createReadStream = function (prefix, opts) {
  return toStream(this.iterator(prefix, opts))
}

function isOptions (opts) {
  return typeof opts === 'object' && !!opts
}

function isPrefix (key, prefix) {
  if (prefix.length && prefix[0] === '/') prefix = prefix.slice(1)
  return key.slice(0, prefix.length) === prefix
}

function normalizeKey (key) {
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}

function noop () {}

function getAllPtrs (self, ptrs) {
  return ptrs.map(x => getPtr(self, x))
}

function getPtr (self, ptr) {
  return self._feeds[ptr.feed][ptr.seq]
}

function pushResult (prefixed, results, key, head) {
  if (prefixed && isPrefix(head.key, key)) return push(results, head)
  if (head.key === key) return push(results, head)
}

function push (results, node) {
  results.push(node)
}

function getLocks (nodes, feedCount) {
  var locks = new Array(feedCount)
  for (var feedId = 0; feedId < feedCount; feedId++) {
    locks[feedId] = getHighestClock(nodes, feedId)
  }
  return locks
}

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

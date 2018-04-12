var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var options = require('./options')
var hash = require('./hash')
var LRU = require('lru')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  nanoiterator.call(this)

  var cacheMax = opts.cacheSize || 128

  this._keyCache = new LRU(cacheMax)
  this._db = db
  this._prefix = prefix
  this.queue = []
  this.queueNeedsSorting = true
  this._end = 0
  this._start = 0
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

    self._hash = hash(self._prefix, false)
    self._start = self._hash.length
    self._end = Infinity
    self.queue = heads.map(h => ({ node: h, index: 0 }))
    cb()
  })
}

Iterator.prototype._next = function (cb) {
  if (!this.queue.length) {
    cb(null)
    return
  }
  // sort stream queue first to ensure that you always get the latest node
  // this requires offsetting feeds sequences based on when it started in relation to others
  // console.log('next', this.queue.map(n => n.node.key))
  if (this.queueNeedsSorting) {
    this.queue.sort(sortStackByClockAndSeq)
    this.queueNeedsSorting = false
    // console.log('sorted', this.queue.map(n => n.node.key))
  }
  var data = this.queue.pop()
  var node = data.node
  this._readNext(node, data.index, (err, match) => {
    if (err) {
      return cb(err)
    }
    if (!match) return this._next(cb)
    // check if really a match and not encountered before
    this._check(node, (err, matchingNode) => {
      if (err) return cb(err)
      if (!matchingNode) this._next(cb)
      else {
        this._keyCache.set(node.key, true)
        // console.log('callbak')
        return cb(null, matchingNode)
      }
    })
  })
}

Iterator.prototype._check = function (node, cb) {
  // is it actually a match and not a collision
  if (!(node && node.key && node.key.indexOf(this._prefix) === 0)) return cb(null)
  // have we encountered this node before
  if (this._keyCache.get(node.key)) return cb(null)
  // it is not in the cache but might still be a duplicate if cache is full
  // if (keyCache.length === cacheMax) {
  // so check if this is the first instance of the node
  // TODO: Atm this is a bit of a hack to get conflicting values
  // ideally this should not need to retraverse the trie.
  // Potential issue here when db is updated after stream was created!
  return this._db.get(node.key, (err, latest) => {
    if (err) cb(err)
    if (sortNodesByClock(node, Array.isArray(latest) ? latest[0] : latest) >= 0) {
      cb(null, latest)
    } else {
      cb(null)
    }
  })
}

Iterator.prototype._readNext = function readNext (node, i, cb) {
  var writers = this._db._writers
  var trie
  var missing = 0
  var error
  var vals
  for (; i < this._hash.length - 1; i++) {
    if (node.path[i] === this._hash.path[i]) continue
    // check trie
    trie = node.trie[i]
    if (!trie) {
      return cb(null)
    }
    vals = trie[this._hash[i]]
    // not found
    if (!vals || !vals.length) {
      return cb(null)
    }

    missing = vals.length
    error = null
    for (var j = 0; j < vals.length; j++) {
      // fetch potential
      writers[vals[j].feed].get(vals[j].seq, (err, val) => {
        if (err) {
          error = err
        } else {
          this._pushToQueue({ node: val, index: i })
        }
        missing--
        if (!missing) {
          cb(error)
        }
      })
    }
    return
  }

  // Traverse the rest of the node's trie, recursively,
  // hunting for more nodes with the desired prefix.
  for (; i < node.trie.length; i++) {
    trie = node.trie[i] || []
    for (j = 0; j < trie.length; j++) {
      var entrySet = trie[j] || []
      for (var el = 0; el < entrySet.length; el++) {
        var entry = entrySet[el]
        missing++
        writers[entry.feed].get(entry.seq, (err, val) => {
          if (err) {
            error = err
          } else if (val.key && val.value) {
            this._pushToQueue({ node: val, index: i + 1 })
          }
          missing--
          if (!missing) {
            if (i < node.trie.length) {
              this._pushToQueue({ node: node, index: i + 1 })
              cb(error, false)
            } else {
              cb(error, true)
            }
          }
        })
      }
    }
    if (missing > 0) return
  }
  return cb(null, true)
}

Iterator.prototype._pushToQueue = function (item) {
  if (!this.queueNeedsSorting && this.queue.length) {
    this.queueNeedsSorting = sortStackByClockAndSeq(this.queue[this.queue.length - 1], item) < 0
  }
  this.queue.push(item)
}

Iterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
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
  a = a.node
  b = b.node
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

var toStream = require('nanoiterator/to-stream')
var mutexify = require('mutexify')
var hash = require('./lib/hash')
var iterator = require('./lib/iterator')
var differ = require('./lib/differ')
var changes = require('./lib/changes')
var get = require('./lib/get')

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
  this._lock = mutexify()
}

DB.prototype.snapshot = function () {
  var snapshot = new DB({id: this._id})
  snapshot._feeds = this._feeds.map(f => f.slice(0))
  snapshot._snapshot = true
  return snapshot
}

DB.prototype.heads = function (cb) {
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

  process.nextTick(cb, null, heads.filter(x => x))
}

DB.prototype.put = function (key, val, cb) {
  if (!cb) cb = noop
  if (this._snapshot) {
    return process.nextTick(cb, new Error('Cannot put on a snapshot'))
  }

  var self = this

  key = normalizeKey(key)

  this._lock(function (release) {
    self.heads(function (err, heads) {
      if (err) return unlock(err)
      self._put(key, val, heads, unlock)
    })

    function unlock (err) {
      release(cb, err)
    }
  })
}

DB.prototype._put = function (key, val, heads, cb) {
  var path = hash(key, true)
  var writable = this._feeds[this._id]
  var clock = []

  for (var i = 0; i < this._feeds.length; i++) {
    clock[i] = (this._feeds[i] || []).length
    if (i === this._id) clock[i]++
  }

  if (!writable) writable = this._feeds[this._id] = []

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
    this._putFromHead(node, 0, heads[j])
  }

  writable.push(node)
  process.nextTick(cb, null)
}

function inspect () {
  return `Node(key=${this.key}, value=${this.value}, seq=${this.seq}, feed=${this.feed})`
}

DB.prototype._putFromHead = function (node, i, head) {
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
          this._putFromHead(node, i + 1, this._feeds[remoteValues[l].feed][remoteValues[l].seq])
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

  this.heads(function (err, heads) {
    if (err) return cb(err)
    get(self, heads, normalizeKey(key), opts, cb)
  })
}

// Used by ./lib/*
DB.prototype._getPointer = function (feed, seq, cb) {
  process.nextTick(cb, null, this._feeds[feed][seq])
}

// Used by ./lib/*
DB.prototype._getAllPointers = function (ptrs, cb) {
  var results = new Array(ptrs.length)
  var error = null
  var missing = results.length

  if (!missing) return process.nextTick(cb, null, results)

  for (var i = 0; i < ptrs.length; i++) {
    var ptr = ptrs[i]
    this._getPointer(ptr.feed, ptr.seq, onnode)
  }

  function onnode (err, node) {
    if (err) error = err
    else results[indexOf(ptrs, node)] = node
    if (--missing) return
    if (error) cb(error, null)
    else cb(null, results)
  }
}

function indexOf (ptrs, ptr) {
  for (var i = 0; i < ptrs.length; i++) {
    var p = ptrs[i]
    if (ptr.feed === p.feed && ptr.seq === p.seq) return i
  }
  return -1
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

function normalizeKey (key) {
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}

function noop () {}

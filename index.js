var hypercore = require('hypercore')
var ram = require('random-access-memory')
var sodium = require('sodium-universal')
var hash = require('./lib/hash')
var iterator = require('./lib/iterator')

module.exports = DB

function DB (opts) {
  if (!(this instanceof DB)) return new DB(opts)
  if (!opts) opts = {}

  this._id = opts.id || 0
  this._feeds = []
  this._length = -1
  this._feeds[this._id] = []
  this._map = opts.map || null
  this._reduce = opts.reduce || null
}

DB.prototype.snapshot = function () {
  var snapshot = new DB()
  snapshot._feeds = this._feeds
  snapshot._length = this._feeds.length
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
  if (this._length > -1) return process.nextTick(cb, new Error('Cannot put on a snapshot'))

  key = normalizeKey(key)
  var path = hash(key, true)
  var writable = this._feeds[this._id]
  var clock = []

  for (var i = 0; i < this._feeds.length; i++) {
    clock[i] = (this._feeds[i] || []).length
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
    trie: []
  }

  if (!heads.length) {
    writable.push(node)
    return cb(null)
  }

  for (var i = 0; i < heads.length; i++) {
    this._put(node, heads[i])
  }

  writable.push(node)
  cb(null)
}

DB.prototype._put = function (node, head) {
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

  for (var i = 0; i < node.path.length; i++) {
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
        for (var k = 0; k < remoteValues.length; k++) {
          this._put(node, this._feeds[remoteValues[k].feed][remoteValues[k].seq])
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
  key = normalizeKey(key)

  var results = []
  var heads = this.heads()

  for (var i = 0; i < heads.length; i++) {
    this._get(key, opts, heads[i], results)
  }

  if (this._map) results = results.map(this._map)
  if (this._reduce) results = results.length ? results.reduce(this._reduce) : null

  process.nextTick(cb, null, results)
}

DB.prototype._get = function (key, opts, head, results) {
  var prefixed = !!(opts && opts.prefix)

  // If no head -> 404
  if (!head) return cb(null, null)

  // Do not terminate the hash if it is a prefix search
  var path = hash(key, !prefixed)

  // We want to find the key closest to our path.
  // At max, we need to go through path.length iterations
  for (var i = 0; i < path.length; i++) {
    var val = path[i]
    if (head.path[i] === val) continue

    // We need a closer node. See if the trie has one that
    // matches the path value
    var remoteBucket = head.trie[i] || []
    var remoteValues = remoteBucket[val] || []

    // No closer ones -> 404
    if (!remoteValues.length) return

    if (remoteValues.length > 1) {
      console.log('get fork', remoteValues)
      process.exit(1)
    }

    // Recursive from a closer node
    head = this._feeds[remoteValues[0].feed][remoteValues[0].seq]
  }

  pushResult(prefixed, results, key, head)

  // check if we had a collision, or similar (our last bucket contains more stuff)

  var last = head.trie[path.length - 1]
  var lastValues = last && last[path[path.length - 1]]
  if (!lastValues) return

  for (var j = 0; j < lastValues.length; j++) {
    var val = this._feeds[lastValues[j].feed][lastValues[j].seq]
    pushResult(prefixed, results, key, val)
  }
}

function pushResult (prefixed, results, key, head) {
  if (prefixed && isPrefix(head.key, key)) return push(results, head)
  if (head.key === key) return push(results, head)
}

function push (results, node) {
  results.push(node)
}

DB.prototype.list = function (prefix, opts, cb) {
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

DB.prototype.iterator = function (prefix, opts) {
  return iterator(this, normalizeKey(prefix), opts)
}

DB.prototype.createReadStream = function (prefix, opts) {
  var ite = this.iterator(prefix, opts)
  var from = require('from2')

  return from.obj(read)

  function read (size, cb) {
    ite.next(cb)
  }
}

function isPrefix (key, prefix) {
  if (!prefix.length || prefix[prefix.length - 1] !== '/') prefix += '/'
  return key.slice(0, prefix.length) === prefix
}

function normalizeKey (key) {
  if (!key.length) return '/'
  return key[0] === '/' ? key : '/' + key
}

function noop () {}

function Thread () {

}


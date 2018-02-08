var alru = require('array-lru')
var messages = require('./messages')
var hash = require('./hash')
var trie = require('./trie')

module.exports = Writer

function Writer (db, feed, id) {
  if (!(this instanceof Writer)) return new Writer(db, feed, id)

  this.id = id
  this.key = feed.key
  this.cache = alru(8192)
  this.owner = true
  this.feed = feed

  this._feedSeq = 0
  this._encodeMap = []
  this._decodeMap = []
  this._db = db
}

Writer.prototype.get = function (seq, cb) {
  var self = this
  var node = this.cache.get(seq)
  if (node) return process.nextTick(cb, null, node, false)

  this.feed.get(seq, function (err, val) {
    if (err) return cb(err)
    var node = messages.Node.decode(val)
    if (node.clock.length <= self._decodeMap.length) return cb(null, self._wrapNode(node, seq), false)
    self._load(seq, node, cb)
  })
}

Writer.prototype.head = function (cb) {
  var self = this

  this.feed.ready(function (err) {
    if (err) return cb(err)

    var len = self._length()
    if (!len) return process.nextTick(cb, null, null, false)
    self.get(len - 1, cb)
  })
}

Writer.prototype._load = function (seq, node, cb) {
  var self = this
  this.feed.get(node.feedSeq, function (err, val) {
    if (err) return cb(err, null, false)
    self._update(messages.Node.decode(val).feeds)
    if (node.clock.length > self._decodeMap.length) return cb(new Error('Invalid feed'), null, false)
    cb(null, self._wrapNode(node, seq), true)
  })
}

Writer.prototype._update = function (feeds) {
  this._decodeMap = new Array(feeds.length)
  this._encodeMap = new Array(feeds.length)
  for (var i = 0; i < feeds.length; i++) {
    var w = this._db._createWriter(feeds[i].key)
    this._decodeMap[i] = w.id
    this._encodeMap[w.id] = i
  }
}

Writer.prototype.batch = function (nodes, cb) {
  var bufs = new Array(nodes.length)
  for (var i = 0; i < bufs.length; i++) bufs[i] = this._preappend(nodes[i])
  this.feed.append(bufs, cb)
}

Writer.prototype.append = function (node, cb) {
  this.feed.append(this._preappend(node), cb)
}

Writer.prototype._preappend = function (node) {
  var feeds = null
  var clock = new Array(node.clock.length)

  if (this._encodeClock(node.clock, clock)) {
    feeds = new Array(clock.length)
    this._feedSeq = node.seq
    for (var i = 0; i < this._db._writers.length; i++) {
      var w = this._db._writers[i]
      feeds[this._encodeMap[i]] = {
        key: w.key,
        owner: w.owner
      }
    }
  }

  node.feeds = feeds || []
  node.feedSeq = this._feedSeq

  var buf = messages.Node.encode({
    key: node.key,
    value: this._db._codec ? this._db._codec.encode(node.value) : node.value,
    clock: clock,
    trie: trie.encode(node.trie, this._encodeMap),
    feeds: feeds,
    feedSeq: this._feedSeq
  })

  return buf
}

Writer.prototype._decodeClock = function (clock) {
  var mapped = new Array(clock.length)
  for (var i = 0; i < clock.length; i++) mapped[this._decodeMap[i]] = clock[i]
  return mapped
}

Writer.prototype._encodeClock = function (clock, mapped) {
  var changed = false

  for (var i = 0; i < clock.length; i++) {
    if (this._encodeMap[i] === undefined) {
      var id = this._decodeMap.push(i) - 1
      this._encodeMap[i] = id
      this._decodeMap[id] = i
      changed = true
    }
    mapped[this._encodeMap[i]] = clock[i]
  }

  return changed
}

Writer.prototype._wrapNode = function (node, seq) {
  node.feed = this.id
  node.seq = seq
  node.path = hash(node.key, true)
  node.trie = trie.decode(node.trie, this._decodeMap)
  if (this._db._codec) node.value = this._db._codec.decode(node.value)
  this.cache.set(seq, node)
  return node
}

Writer.prototype._length = function () {
  var len = this.feed.length

  for (var i = 0; i < this.feed.peers.length; i++) {
    var remoteLength = this.feed.peers[i].remoteLength
    if (remoteLength > len) len = remoteLength
  }

  return len
}

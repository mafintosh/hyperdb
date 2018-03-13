var hypercore = require('hypercore')
var protocol = require('hypercore-protocol')
var thunky = require('thunky')
var remove = require('unordered-array-remove')
var toStream = require('nanoiterator/to-stream')
var varint = require('varint')
var mutexify = require('mutexify')
var codecs = require('codecs')
var raf = require('random-access-file')
var path = require('path')
var util = require('util')
var events = require('events')
var hash = require('./lib/hash')
var iterator = require('./lib/iterator')
var differ = require('./lib/differ')
var history = require('./lib/history')
var get = require('./lib/get')
var put = require('./lib/put')
var messages = require('./lib/messages')
var trie = require('./lib/trie-encoding')
var watch = require('./lib/watch')

module.exports = HyperDB

function HyperDB (storage, key, opts) {
  if (!(this instanceof HyperDB)) return new HyperDB(storage, key, opts)
  events.EventEmitter.call(this)

  if (isOptions(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}
  if (opts.one) opts.reduce = reduceFirst

  var checkout = opts.checkout

  this.key = typeof key === 'string' ? Buffer.from(key, 'hex') : key
  this.discoveryKey = this.key ? hypercore.discoveryKey(this.key) : null
  this.source = checkout ? checkout.source : null
  this.local = checkout ? checkout.local : null
  this.localContent = checkout ? checkout.localContent : null
  this.feeds = checkout ? checkout.feeds : []
  this.contentFeeds = checkout ? checkout.contentFeeds : (opts.contentFeed ? [] : null)
  this.ready = thunky(this._ready.bind(this))
  this.opened = false
  this.sparse = !!opts.sparse
  this.sparseContent = opts.sparseContent !== undefined ? !!opts.sparseContent : this.sparse

  this._storage = createStorage(storage)
  this._contentStorage = opts.contentFeed ? this._storage : null
  this._writers = checkout ? checkout._writers : []
  this._watching = checkout ? checkout._watching : []
  this._replicating = []
  this._localWriter = null
  this._byKey = new Map()
  this._heads = null
  this._version = opts.version || null
  this._checkout = checkout || null
  this._lock = mutexify()
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._valueEncoding = codecs(opts.valueEncoding || 'binary')

  this.ready()
}

util.inherits(HyperDB, events.EventEmitter)

HyperDB.prototype.put = function (key, val, cb) {
  if (!cb) cb = noop

  if (this._checkout) {
    return process.nextTick(cb, new Error('Cannot put on a checkout'))
  }

  var self = this

  key = normalizeKey(key)

  this._lock(function (release) {
    var clock = self._clock()
    self.heads(function (err, heads) {
      if (err) return unlock(err)
      put(self, clock, heads, key, val, unlock)
    })

    function unlock (err, node) {
      release(cb, err, node)
    }
  })
}

HyperDB.prototype.del = function (key, cb) {
  this.put(key, null, cb)
}

HyperDB.prototype.watch = function (key, cb) {
  if (typeof key === 'function') return this.watch('', key)
  return watch(this, normalizeKey(key), cb)
}

HyperDB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)

  var self = this

  this.heads(function (err, heads) {
    if (err) return cb(err)
    get(self, heads, normalizeKey(key), opts, cb)
  })
}

HyperDB.prototype.version = function (cb) {
  var self = this

  this.heads(function (err, heads) {
    if (err) return cb(err)

    var buffers = []

    for (var i = 0; i < heads.length; i++) {
      buffers.push(self.feeds[heads[i].feed].key)
      buffers.push(Buffer.from(varint.encode(heads[i].seq)))
    }

    cb(null, Buffer.concat(buffers))
  })
}

HyperDB.prototype.checkout = function (version, opts) {
  if (!opts) opts = {}
  return new HyperDB(this._storage, this.key, {
    checkout: this,
    version: version,
    map: opts.map !== undefined ? opts.map : this._map,
    reduce: opts.reduce !== undefined ? opts.reduce : this._reduce
  })
}

HyperDB.prototype.snapshot = function (opts) {
  return this.checkout(null, opts)
}

HyperDB.prototype.heads = function (cb) {
  if (!this.opened) return readyAndHeads(this, cb)
  if (this._heads) return process.nextTick(cb, null, this._heads)

  var self = this
  var len = this._writers.length
  var missing = len
  var error = null
  var nodes = new Array(len)

  for (var i = 0; i < len; i++) {
    this._writers[i].head(onhead)
  }

  function onhead (err, head, i) {
    if (err) error = err
    else nodes[i] = head

    if (--missing) return

    if (error) return cb(error)
    if (len !== self._writers.length) return self.heads(cb)

    if (nodes.length === 1) return cb(null, nodes[0] ? nodes : [])
    cb(null, filterHeads(nodes))
  }
}

HyperDB.prototype._index = function (key) {
  if (key.key) key = key.key
  for (var i = 0; i < this.feeds.length; i++) {
    if (this.feeds[i].key.equals(key)) return i
  }
  return -1
}

HyperDB.prototype.authenticated = function (key) {
  return this._index(key) > -1
}

HyperDB.prototype.authorize = function (key, cb) {
  if (!cb) cb = noop

  var self = this

  this.heads(function (err) { // populates .feeds to be up to date
    if (err) return cb(err)
    self._addWriter(key, function (err) {
      if (err) return cb(err)
      self.put('', null, cb)
    })
  })
}

HyperDB.prototype.replicate = function (opts) {
  if (!opts) opts = {}

  var self = this
  var expectedFeeds = this._writers.length
  var factor = this.contentFeeds ? 2 : 1

  opts.expectedFeeds = expectedFeeds * factor

  if (!opts.stream) opts.stream = protocol(opts)
  var stream = opts.stream

  if (!opts.live) stream.on('prefinalize', prefinalize)

  this.ready(onready)

  // bootstrap content feeds
  if (this.contentFeeds && !this.contentFeeds[0]) this._writers[0].get(0, noop)

  return stream

  function onready (err) {
    if (err) return stream.destroy(err)
    if (stream.destroyed) return

    var i = 0
    var j = 0

    self._replicating.push(replicate)
    stream.on('close', onclose)

    replicate()

    function replicate () {
      for (; i < self.feeds.length; i++) {
        self.feeds[i].replicate(opts)
      }

      if (!self.contentFeeds) return

      for (; j < self.contentFeeds.length; j++) {
        if (!self.contentFeeds[j]) return
        self.contentFeeds[j].replicate(opts)
      }
    }

    function onclose () {
      var i = self._replicating.indexOf(replicate)
      remove(self._replicating, i)
    }
  }

  function prefinalize (cb) {
    self.heads(function (err) {
      if (err) return cb(err)
      stream.expectedFeeds += factor * (self._writers.length - expectedFeeds)
      expectedFeeds = self._writers.length
      cb()
    })
  }
}

HyperDB.prototype._clock = function () {
  var clock = new Array(this._writers.length)

  for (var i = 0; i < clock.length; i++) {
    var w = this._writers[i]
    clock[i] = w === this._localWriter ? w._clock : w.length()
  }

  return clock
}

HyperDB.prototype._getPointer = function (feed, index, cb) {
  this._writers[feed].get(index, cb)
}

HyperDB.prototype._getAllPointers = function (list, cb) {
  var error = null
  var result = new Array(list.length)
  var missing = result.length

  if (!missing) return process.nextTick(cb, null, result)

  for (var i = 0; i < result.length; i++) {
    this._getPointer(list[i].feed, list[i].seq, done)
  }

  function done (err, node) {
    if (err) error = err
    else result[indexOf(list, node)] = node
    if (!--missing) cb(error, result)
  }
}

HyperDB.prototype._writer = function (dir, key) {
  var writer = key && this._byKey.get(key.toString('hex'))
  if (writer) return writer

  var self = this
  var feed = hypercore(storage, key, {sparse: this.sparse})

  writer = new Writer(self, feed)
  feed.on('append', onappend)

  if (key) addWriter(null)
  else feed.ready(addWriter)

  return writer

  function onappend () {
    for (var i = 0; i < self._watching.length; i++) self._watching[i]._kick()
    self.emit('append', feed)
  }

  function addWriter (err) {
    if (!err) self._byKey.set(feed.key.toString('hex'), writer)
  }

  function storage (name) {
    return self._storage(dir + '/' + name)
  }
}

HyperDB.prototype._addWriter = function (key, cb) {
  var self = this
  var writer = this._writer('peers/' + hypercore.discoveryKey(key).toString('hex'), key)

  writer._feed.ready(function (err) {
    if (err) return cb(err)
    if (self.authenticated(key)) return cb(null)
    self._pushWriter(writer)
    cb(null)
  })
}

HyperDB.prototype._pushWriter = function (writer) {
  writer._id = this._writers.push(writer) - 1
  this.feeds.push(writer._feed)
  if (this.contentFeeds) this.contentFeeds.push(null)

  if (!this.opened) return

  for (var i = 0; i < this._replicating.length; i++) {
    this._replicating[i]()
  }
}

HyperDB.prototype.list = function (prefix, opts, cb) {
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

HyperDB.prototype.history = function () {
  return history(this)
}

HyperDB.prototype.diff = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.diff(other, null, prefix)
  return differ(this, other || checkoutEmpty(this), prefix || '', opts)
}

HyperDB.prototype.iterator = function (prefix, opts) {
  if (isOptions(prefix)) return this.iterator('', prefix)
  return iterator(this, normalizeKey(prefix || ''), opts)
}

HyperDB.prototype.createHistoryStream = function () {
  return toStream(this.history())
}

HyperDB.prototype.createDiffStream = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.createDiffStream(other, '', prefix)
  return toStream(this.diff(other, prefix, opts))
}

HyperDB.prototype.createReadStream = function (prefix, opts) {
  return toStream(this.iterator(prefix, opts))
}

HyperDB.prototype._ready = function (cb) {
  var self = this

  if (this._checkout) {
    if (this._version) this._checkout.heads(onversion)
    else this._checkout.heads(oncheckout)
    return
  }

  if (!this.source) this.source = feed('source', this.key)

  this.source.ready(function (err) {
    if (err) return done(err)
    if (self.source.writable) self.local = self.source
    if (!self.local) self.local = feed('local')

    self.key = self.source.key
    self.discoveryKey = self.source.discoveryKey

    self.local.ready(function (err) {
      if (err) return done(err)

      self._localWriter = self._writers[self.feeds.indexOf(self.local)]
      self._localWriter.head(function (err) {
        if (err) return done(err)
        if (!self._contentStorage || self._localWriter._contentFeed) return done(null)

        self._localWriter._ensureContentFeed(null)
        self.localContent = self._localWriter._contentFeed

        done(null)
      })
    })
  })

  function done (err) {
    if (err) return cb(err)
    self.opened = true
    self.emit('ready')
    cb(null)
  }

  function feed (dir, key) {
    var writer = self._writer(dir, key)
    self._pushWriter(writer)
    return writer._feed
  }

  function onversion (err) {
    if (err) return done(err)

    var offset = 0
    var missing = 0
    var nodes = []
    var error = null

    while (offset < self._version.length) {
      missing++
      var key = self._version.slice(offset, offset + 32)
      var seq = varint.decode(self._version, offset + 32)
      offset += 32 + varint.decode.bytes
      var writer = self._checkout._byKey.get(key.toString('hex'))
      writer.get(seq, onnode)
    }

    if (!missing) oncheckout(null, [])

    function onnode (err, node) {
      if (err) error = err
      else nodes.push(node)
      if (!--missing) oncheckout(error, nodes)
    }
  }

  function oncheckout (err, heads) {
    if (err) return done(err)

    self.opened = true
    self.source = self._checkout.source
    self.local = self._checkout.local
    self.key = self._checkout.key
    self.discoveryKey = self._checkout.discoveryKey
    self._heads = heads

    done(null)
  }
}

function Writer (db, feed) {
  this._id = 0
  this._db = db
  this._feed = feed
  this._contentFeed = null
  this._feeds = 0
  this._feedsMessage = null
  this._feedsLoaded = 0
  this._entry = 0
  this._clock = 0
  this._encodeMap = []
  this._decodeMap = []
  this._checkout = false
  this._length = 0
}

Writer.prototype.append = function (entry, cb) {
  if (!this._clock) this._clock = this._feed.length

  var enc = messages.Entry
  this._entry = this._clock++

  entry.clock[this._id] = this._clock
  entry.seq = this._clock - 1
  entry.feed = this._id
  entry[util.inspect.custom] = inspect

  var mapped = {
    key: entry.key,
    value: null,
    inflate: 0,
    clock: null,
    trie: null,
    feeds: null,
    contentFeed: null
  }

  if (this._needsInflate()) {
    enc = messages.InflatedEntry
    mapped.feeds = this._mapList(this._db.feeds, this._encodeMap, null)
    if (this._db.contentFeeds) mapped.contentFeed = this._db.contentFeeds[this._id].key
    this._feedsMessage = mapped
    this._feedsLoaded = this._feeds = this._entry
    this._updateFeeds()
  }

  mapped.clock = this._mapList(entry.clock, this._encodeMap, 0)
  mapped.inflate = this._feeds
  mapped.trie = trie.encode(entry.trie, this._encodeMap)
  if (entry.value) mapped.value = this._db._valueEncoding.encode(entry.value)

  this._feed.append(enc.encode(mapped), cb)
}

Writer.prototype._needsInflate = function () {
  var msg = this._feedsMessage
  return !msg || msg.feeds.length !== this._db.feeds.length
}

Writer.prototype._maybeUpdateFeeds = function () {
  if (!this._feedsMessage) return
  if (this._decodeMap.length === this._db.feeds.length) return
  if (this._encodeMap.length === this._db.feeds.length) return
  this._updateFeeds()
}

Writer.prototype.get = function (seq, cb) {
  var self = this

  this._feed.get(seq, function (err, val) {
    if (err) return cb(err)

    val = messages.Entry.decode(val)
    val[util.inspect.custom] = inspect
    val.seq = seq
    val.path = hash(val.key, true)
    val.value = val.value && self._db._valueEncoding.decode(val.value)

    if (self._feedsMessage && self._feedsLoaded === val.inflate) {
      self._maybeUpdateFeeds()
      val.feed = self._id
      val.clock = self._mapList(val.clock, self._decodeMap, 0)
      val.trie = trie.decode(val.trie, self._decodeMap)
      return cb(null, val, self._id)
    }

    self._loadFeeds(val, cb)
  })
}

Writer.prototype.head = function (cb) {
  var len = this.length()
  if (!len) return process.nextTick(cb, null, null, this._id)
  this.get(len - 1, cb)
}

Writer.prototype._mapList = function (list, map, def) {
  var mapped = []
  var i
  for (i = 0; i < map.length; i++) mapped[map[i]] = i < list.length ? list[i] : def
  for (; i < list.length; i++) mapped[i] = list[i]
  for (i = 0; i < mapped.length; i++) {
    if (!mapped[i]) mapped[i] = def
  }
  return mapped
}

Writer.prototype._loadFeeds = function (head, cb) {
  var self = this

  if (head.feeds) done(head)
  else this._feed.get(head.inflate, onfeeds)

  function onfeeds (err, buf) {
    if (err) return cb(err)
    done(messages.InflatedEntry.decode(buf))
  }

  function done (msg) {
    if (msg.seq < self._feedsLoaded) return cb(null, head, self._id)

    self._feedsLoaded = msg.seq
    self._feedsMessage = msg
    self._addWriters(head, cb)
  }
}

Writer.prototype._addWriters = function (head, cb) {
  var self = this
  var id = this._id
  var writers = this._feedsMessage.feeds || []
  var missing = writers.length + 1
  var error = null

  for (var i = 0; i < writers.length; i++) {
    this._db._addWriter(writers[i].key, done)
  }

  done(null)

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)
    self._updateFeeds()
    head.feed = self._id
    head.clock = self._mapList(head.clock, self._decodeMap, 0)
    head.trie = trie.decode(head.trie, self._decodeMap)
    cb(null, head, id)
  }
}

Writer.prototype._ensureContentFeed = function (key) {
  if (this._contentFeed) return

  var self = this

  this._contentFeed = hypercore(storage, key, {sparse: this._db.sparseContent})
  if (this._db.contentFeeds) this._db.contentFeeds[this._id] = this._contentFeed

  function storage (name) {
    return self._db._contentStorage('content/' + self._feed.discoveryKey.toString('hex') + '/' + name)
  }
}

Writer.prototype._updateFeeds = function () {
  var i

  if (this._feedsMessage.contentFeed && this._db.contentFeeds && !this._contentFeed) {
    this._ensureContentFeed(this._feedsMessage.contentFeed)
    for (i = 0; i < this._db._replicating.length; i++) {
      this._db._replicating[i]()
    }
  }

  var writers = this._feedsMessage.feeds || []
  var map = new Map()

  for (i = 0; i < this._db.feeds.length; i++) {
    map.set(this._db.feeds[i].key.toString('hex'), i)
  }

  for (i = 0; i < writers.length; i++) {
    var id = map.get(writers[i].key.toString('hex'))
    this._decodeMap[i] = id
    this._encodeMap[id] = i
  }
}

Writer.prototype.length = function () {
  if (this._checkout) return this._length
  return Math.max(this._feed.length, this._feed.remoteLength)
}

function filterHeads (list) {
  var heads = []
  for (var i = 0; i < list.length; i++) {
    if (isHead(list[i], list)) heads.push(list[i])
  }
  return heads
}

function isHead (node, list) {
  if (!node) return false

  var clock = node.seq + 1

  for (var i = 0; i < list.length; i++) {
    var other = list[i]
    if (other === node || !other) continue
    if ((other.clock[node.feed] || 0) >= clock) return false
  }

  return true
}

function checkoutEmpty (db) {
  db = db.checkout(Buffer.from([]))
  return db
}

function readyAndHeads (self, cb) {
  self.ready(function (err) {
    if (err) return cb(err)
    self.heads(cb)
  })
}

function indexOf (list, ptr) {
  for (var i = 0; i < list.length; i++) {
    var p = list[i]
    if (ptr.feed === p.feed && ptr.seq === p.seq) return i
  }
  return -1
}

function isOptions (opts) {
  return typeof opts === 'object' && !!opts && !Buffer.isBuffer(opts)
}

function normalizeKey (key) {
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}

function createStorage (st) {
  if (typeof st === 'function') return st
  return function (name) {
    return raf(path.join(st, name))
  }
}

function reduceFirst (a, b) {
  return a
}

function noop () {}

function inspect () {
  return `Node(key=${this.key}` +
    `, value=${util.inspect(this.value)}` +
    `, seq=${this.seq}` +
    `, feed=${this.feed})` +
    `)`
}

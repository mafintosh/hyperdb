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
var bulk = require('bulk-write-stream')
var events = require('events')
var sodium = require('sodium-universal')
var alru = require('array-lru')
var inherits = require('inherits')
var hash = require('./lib/hash')
var iterator = require('./lib/iterator')
var differ = require('./lib/differ')
var history = require('./lib/history')
var get = require('./lib/get')
var put = require('./lib/put')
var messages = require('./lib/messages')
var trie = require('./lib/trie-encoding')
var watch = require('./lib/watch')
var normalizeKey = require('./lib/normalize')
var derive = require('./lib/derive')

module.exports = HyperDB

function HyperDB (storage, key, opts) {
  if (!(this instanceof HyperDB)) return new HyperDB(storage, key, opts)
  events.EventEmitter.call(this)

  if (isOptions(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}
  if (opts.firstNode) opts.reduce = reduceFirst

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
  this.id = Buffer.alloc(32)
  sodium.randombytes_buf(this.id)

  this._storage = createStorage(storage)
  this._contentStorage = typeof opts.contentFeed === 'function'
    ? opts.contentFeed
    : opts.contentFeed ? this._storage : null
  this._writers = checkout ? checkout._writers : []
  this._watching = checkout ? checkout._watching : []
  this._replicating = []
  this._localWriter = null
  this._byKey = new Map()
  this._heads = opts.heads || null
  this._version = opts.version || null
  this._checkout = checkout || null
  this._lock = mutexify()
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._valueEncoding = codecs(opts.valueEncoding || 'binary')
  this._batching = null
  this._batchingNodes = null
  this._secretKey = opts.secretKey || null
  this._storeSecretKey = opts.storeSecretKey !== false
  this._onwrite = opts.onwrite || null

  this.ready()
}

inherits(HyperDB, events.EventEmitter)

HyperDB.prototype.batch = function (batch, cb) {
  if (!cb) cb = noop

  var self = this

  this._lock(function (release) {
    var clock = self._clock()

    self._batching = []
    self._batchingNodes = []

    self.heads(function (err, heads) {
      if (err) return cb(err)

      var i = 0

      loop(null)

      function loop (err, node) {
        if (err) return done(err)

        if (node) {
          node.path = hash(node.key, true)
          heads = [node]
        }

        if (i === batch.length) {
          self.local.append(self._batching, done)
          return
        }

        var next = batch[i++]
        put(self, clock, heads, next.key, next.value || null, loop)
      }

      function done (err) {
        var nodes = self._batchingNodes
        self._batching = null
        self._batchingNodes = null
        return release(cb, err, nodes)
      }
    })
  })
}

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

  if (typeof version === 'string') {
    version = Buffer.from(version, 'hex')
  }

  if (Array.isArray(version)) {
    opts.heads = version
    version = null
  }

  return new HyperDB(this._storage, this.key, {
    checkout: this,
    version: version,
    map: opts.map !== undefined ? opts.map : this._map,
    reduce: opts.reduce !== undefined ? opts.reduce : this._reduce,
    heads: opts.heads
  })
}

HyperDB.prototype.snapshot = function (opts) {
  return this.checkout(null, opts)
}

HyperDB.prototype.heads = function (cb) {
  if (!this.opened) return readyAndHeads(this, cb)
  if (this._heads) return process.nextTick(cb, null, this._heads)

  // This is a bit of a hack. Basically when the db is empty
  // we wanna wait for data to come in. TODO: We should guarantee
  // that the db always has a single block of data (like a header)
  if (this._waitForUpdate()) {
    this.setMaxListeners(0)
    this.once('remote-update', this.heads.bind(this, cb))
    return
  }

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

HyperDB.prototype._waitForUpdate = function () {
  return this._replicating.length &&
    !this._writers[0].length() &&
    this.local !== this.source
}

HyperDB.prototype._index = function (key) {
  if (key.key) key = key.key
  for (var i = 0; i < this.feeds.length; i++) {
    if (this.feeds[i].key.equals(key)) return i
  }
  return -1
}

HyperDB.prototype.authorized = function (key, cb) {
  var self = this

  this.heads(function (err) {
    if (err) return cb(err)
    // writers[0] is the source, always authed
    cb(null, self._writers[0].authorizes(key, null))
  })
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
  if (!opts.id) opts.id = this.id

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

    self._replicating.push(replicate)
    stream.on('close', onclose)
    stream.on('end', onclose)

    replicate()

    function oncontent () {
      this._contentFeed.replicate(opts)
    }

    function replicate () {
      for (; i < self.feeds.length; i++) {
        self.feeds[i].replicate(opts)
        if (!self.contentFeeds) continue
        var w = self._writers[i]
        if (w._contentFeed) w._contentFeed.replicate(opts)
        else w.once('content-feed', oncontent)
      }
    }

    function onclose () {
      var i = self._replicating.indexOf(replicate)
      if (i > -1) remove(self._replicating, i)
      for (i = 0; i < self._writers.length; i++) {
        self._writers[i].removeListener('content-feed', oncontent)
      }
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

HyperDB.prototype._getPointer = function (feed, index, isPut, cb) {
  if (isPut && this._batching && feed === this._localWriter._id && index >= this._localWriter._feed.length) {
    process.nextTick(cb, null, this._batchingNodes[index - this._localWriter._feed.length])
    return
  }
  this._writers[feed].get(index, cb)
}

HyperDB.prototype._getAllPointers = function (list, isPut, cb) {
  var error = null
  var result = new Array(list.length)
  var missing = result.length

  if (!missing) return process.nextTick(cb, null, result)

  for (var i = 0; i < result.length; i++) {
    this._getPointer(list[i].feed, list[i].seq, isPut, done)
  }

  function done (err, node) {
    if (err) error = err
    else result[indexOf(list, node)] = node
    if (!--missing) cb(error, result)
  }
}

HyperDB.prototype._writer = function (dir, key, opts) {
  var writer = key && this._byKey.get(key.toString('hex'))
  if (writer) return writer

  opts = Object.assign({}, opts, {
    sparse: this.sparse,
    onwrite: this._onwrite ? onwrite : null
  })

  var self = this
  var feed = hypercore(storage, key, opts)

  writer = new Writer(self, feed)
  feed.on('append', onappend)
  feed.on('remote-update', onremoteupdate)
  feed.on('sync', onreloadhead)

  if (key) addWriter(null)
  else feed.ready(addWriter)

  return writer

  function onwrite (index, data, peer, cb) {
    if (peer) peer.maxRequests++
    if (index >= writer._writeLength) writer._writeLength = index + 1
    writer._writes.set(index, data)
    writer._decode(index, data, function (err, entry) {
      if (err) return done(cb, index, peer, err)
      self._onwrite(entry, peer, function (err) {
        done(cb, index, peer, err)
      })
    })
  }

  function done (cb, index, peer, err) {
    if (peer) peer.maxRequests--
    writer._writes.delete(index)
    cb(err)
  }

  function onremoteupdate () {
    self.emit('remote-update', feed, writer._id)
  }

  function onreloadhead () {
    // read writer head to see if any new writers are added on full sync
    writer.head(noop)
  }

  function onappend () {
    for (var i = 0; i < self._watching.length; i++) self._watching[i]._kick()
    self.emit('append', feed, writer._id)
  }

  function addWriter (err) {
    if (!err) self._byKey.set(feed.key.toString('hex'), writer)
  }

  function storage (name) {
    return self._storage(dir + '/' + name, {feed})
  }
}

HyperDB.prototype._getWriter = function (key) {
  return this._byKey.get(key.toString('hex'))
}

HyperDB.prototype._addWriter = function (key, cb) {
  var self = this
  var writer = this._writer('peers/' + hypercore.discoveryKey(key).toString('hex'), key)

  writer._feed.ready(function (err) {
    if (err) return cb(err)
    if (self._index(key) <= -1) self._pushWriter(writer)
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

HyperDB.prototype.history = function (opts) {
  return history(this, opts)
}

HyperDB.prototype.diff = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.diff(other, null, prefix)
  return differ(this, other || checkoutEmpty(this), prefix || '', opts)
}

HyperDB.prototype.iterator = function (prefix, opts) {
  if (isOptions(prefix)) return this.iterator('', prefix)
  return iterator(this, normalizeKey(prefix || ''), opts)
}

HyperDB.prototype.createHistoryStream = function (opts) {
  return toStream(this.history(opts))
}

HyperDB.prototype.createDiffStream = function (other, prefix, opts) {
  if (isOptions(prefix)) return this.createDiffStream(other, '', prefix)
  return toStream(this.diff(other, prefix, opts))
}

HyperDB.prototype.createReadStream = function (prefix, opts) {
  return toStream(this.iterator(prefix, opts))
}

HyperDB.prototype.createWriteStream = function (cb) {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    var flattened = []
    for (var i = 0; i < batch.length; i++) {
      var content = batch[i]
      if (Array.isArray(content)) {
        for (var j = 0; j < content.length; j++) {
          flattened.push(content[j])
        }
      } else {
        flattened.push(content)
      }
    }
    self.batch(flattened, cb)
  }
}

HyperDB.prototype._ready = function (cb) {
  var self = this

  if (this._checkout) {
    if (this._heads) oncheckout(null, this._heads)
    else if (this._version) this._checkout.heads(onversion)
    else this._checkout.heads(oncheckout)
    return
  }

  if (!this.source) {
    this.source = feed('source', this.key, {
      secretKey: this._secretKey,
      storeSecretKey: this._storeSecretKey
    })
  }

  this.source.ready(function (err) {
    if (err) return done(err)
    if (self.source.writable) self.local = self.source
    if (!self.local) self.local = feed('local')

    self.key = self.source.key
    self.discoveryKey = self.source.discoveryKey

    self.local.ready(function (err) {
      if (err) return done(err)

      self._localWriter = self._writers[self.feeds.indexOf(self.local)]

      if (self._contentStorage) {
        self._localWriter._ensureContentFeed(null)
        self.localContent = self._localWriter._contentFeed
      }

      self._localWriter.head(function (err) {
        if (err) return done(err)
        if (!self.localContent) return done(null)
        self.localContent.ready(done)
      })
    })
  })

  function done (err) {
    if (err) return cb(err)
    self.opened = true
    self.emit('ready')
    cb(null)
  }

  function feed (dir, key, feedOpts) {
    var writer = self._writer(dir, key, feedOpts)
    self._pushWriter(writer)
    return writer._feed
  }

  function onversion (err) {
    if (err) return done(err)

    var offset = 0
    var missing = 0
    var nodes = []
    var error = null

    if (typeof self._version === 'number') {
      missing = 1
      self._checkout._writers[0].get(self._version, onnode)
      return
    }

    while (offset < self._version.length) {
      var key = self._version.slice(offset, offset + 32)
      var seq = varint.decode(self._version, offset + 32)
      offset += 32 + varint.decode.bytes
      var writer = self._checkout._byKey.get(key.toString('hex'))
      if (!writer) {
        error = new Error('Invalid version')
        continue
      }
      missing++
      writer.get(seq, onnode)
    }

    if (!missing) oncheckout(error, [])

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
    self.localContent = self._checkout.localContent
    self.key = self._checkout.key
    self.discoveryKey = self._checkout.discoveryKey
    self._heads = heads

    done(null)
  }
}

function Writer (db, feed) {
  events.EventEmitter.call(this)

  this._id = 0
  this._db = db
  this._feed = feed
  this._contentFeed = null
  this._feeds = 0
  this._feedsMessage = null
  this._feedsLoaded = -1
  this._entry = 0
  this._clock = 0
  this._encodeMap = []
  this._decodeMap = []
  this._checkout = false
  this._length = 0

  this._cache = alru(4096)

  this._writes = new Map()
  this._writeLength = 0

  this.setMaxListeners(0)
}

inherits(Writer, events.EventEmitter)

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

  if (this._db._batching) {
    this._db._batching.push(enc.encode(mapped))
    this._db._batchingNodes.push(entry)
    return cb(null)
  }

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

Writer.prototype._decode = function (seq, buf, cb) {
  var val = messages.Entry.decode(buf)
  val[util.inspect.custom] = inspect
  val.seq = seq
  val.path = hash(val.key, true)
  val.value = val.value && this._db._valueEncoding.decode(val.value)

  if (this._feedsMessage && this._feedsLoaded === val.inflate) {
    this._maybeUpdateFeeds()
    val.feed = this._id
    if (val.clock.length > this._decodeMap.length) {
      return cb(new Error('Missing feed mappings'))
    }
    val.clock = this._mapList(val.clock, this._decodeMap, 0)
    val.trie = trie.decode(val.trie, this._decodeMap)
    this._cache.set(val.seq, val)
    return cb(null, val, this._id)
  }

  this._loadFeeds(val, buf, cb)
}

Writer.prototype.get = function (seq, cb) {
  var self = this

  var cached = this._cache.get(seq)
  if (cached) return process.nextTick(cb, null, cached, this._id)

  this._getFeed(seq, function (err, val) {
    if (err) return cb(err)
    self._decode(seq, val, cb)
  })
}

Writer.prototype._getFeed = function (seq, cb) {
  if (this._writes && this._writes.size) {
    var buf = this._writes.get(seq)
    if (buf) return process.nextTick(cb, null, buf)
  }
  this._feed.get(seq, cb)
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

Writer.prototype._loadFeeds = function (head, buf, cb) {
  var self = this

  if (head.feeds) done(head)
  else if (head.inflate === head.seq) onfeeds(null, buf)
  else this._getFeed(head.inflate, onfeeds)

  function onfeeds (err, buf) {
    if (err) return cb(err)
    done(messages.InflatedEntry.decode(buf))
  }

  function done (msg) {
    var seq = head.inflate
    if (seq > self._feedsLoaded) {
      self._feedsLoaded = self._feeds = seq
      self._feedsMessage = msg
    }
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
    if (head.clock.length > self._decodeMap.length) {
      return cb(new Error('Missing feed mappings'))
    }
    head.clock = self._mapList(head.clock, self._decodeMap, 0)
    head.trie = trie.decode(head.trie, self._decodeMap)
    self._cache.set(head.seq, head)
    cb(null, head, id)
  }
}

Writer.prototype._ensureContentFeed = function (key) {
  if (this._contentFeed) return

  var self = this
  var secretKey = null

  if (!key) {
    var pair = derive(this._db.local.secretKey)
    secretKey = pair.secretKey
    key = pair.publicKey
  }

  this._contentFeed = hypercore(storage, key, {
    sparse: this._db.sparseContent,
    storeSecretKey: false,
    secretKey
  })

  if (this._db.contentFeeds) this._db.contentFeeds[this._id] = this._contentFeed
  this.emit('content-feed')

  function storage (name) {
    name = 'content/' + self._feed.discoveryKey.toString('hex') + '/' + name
    return self._db._contentStorage(name, {
      metadata: self._feed,
      feed: self._contentFeed
    })
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

Writer.prototype.authorizes = function (key, visited) {
  if (!visited) visited = new Array(this._db._writers.length)

  if (this._feed.key.equals(key)) return true
  if (!this._feedsMessage || visited[this._id]) return false
  visited[this._id] = true

  var feeds = this._feedsMessage.feeds || []
  for (var i = 0; i < feeds.length; i++) {
    var authedKey = feeds[i].key
    if (authedKey.equals(key)) return true
    var authedWriter = this._db._getWriter(authedKey)
    if (authedWriter.authorizes(key, visited)) return true
  }

  return false
}

Writer.prototype.length = function () {
  if (this._checkout) return this._length
  return Math.max(this._writeLength, Math.max(this._feed.length, this._feed.remoteLength))
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

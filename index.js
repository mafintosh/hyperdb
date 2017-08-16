var hash = require('./lib/hash')
var writer = require('./lib/writer')
var hypercore = require('hypercore')
var remove = require('unordered-array-remove')
var raf = require('random-access-file')
var mutexify = require('mutexify')
var thunky = require('thunky')
var codecs = require('codecs')
var events = require('events')
var inherits = require('inherits')
var toBuffer = require('to-buffer')
var protocol = null // lazy load on replicate

module.exports = DB

function DB (storage, key, opts) {
  if (!(this instanceof DB)) return new DB(storage, key, opts)
  events.EventEmitter.call(this)

  if (isOptions(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  var self = this

  this.key = key ? toBuffer(key, 'hex') : null
  this.discoveryKey = null
  this.local = null
  this.source = null
  this.opened = false
  this.sparse = !!opts.sparse

  this._codec = opts.valueEncoding ? codecs(opts.valueEncoding) : null
  this._storage = typeof storage === 'string' ? fileStorage : storage
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._writers = []
  this._lock = mutexify() // unneeded once we support batching
  this._localWriter = null

  this.ready = thunky(open)
  this.ready()

  function open (cb) {
    self._open(cb)
  }

  function fileStorage (name) {
    return raf(name, {directory: storage})
  }
}

inherits(DB, events.EventEmitter)

DB.prototype._heads = function (cb) {
  var result = []
  var error = null
  var self = this
  var missing = 0
  var i = 0

  this.ready(onready)

  function onready (err) {
    if (err) return cb(err)

    for (; i < self._writers.length; i++) {
      missing++
      self._writers[i].head(onhead)
    }
  }

  function onhead (err, val, updated) {
    if (updated) onready(null)

    if (err) error = err
    else if (val) result[val.feed] = val
    if (--missing) return

    if (error) return cb(error)

    for (var i = 0; i < result.length; i++) {
      var head = result[i]
      if (!head) continue

      for (var j = 0; j < head.clock.length; j++) {
        if (result[j] && result[j].seq < head.clock[j]) result[j] = null
      }
    }

    cb(null, result.filter(Boolean))
  }
}

DB.prototype.authorize = function (key, cb) {
  this._createWriter(key)
  this.put('', '', cb)
}

DB.prototype.replicate = function (opts) {
  if (!protocol) protocol = require('hypercore-protocol')
  if (!opts) opts = {}

  var len = this._writers.length
  opts.expectedFeeds = len

  var self = this
  var stream = protocol(opts)

  opts.stream = stream

  if (!opts.live) this._heads(noop)
  this.ready(function (err) {
    if (err) return stream.destroy(err)
    if (stream.destroyed) return

    for (var i = 0; i < self._writers.length; i++) {
      self._writers[i].feed.replicate(opts)
    }

    if (!opts.live) self.on('append', onappend)
    self.on('_writer', onwriter)
    stream.on('close', onclose)

    function onclose () {
      self.removeListener('_writer', onwriter)
      self.removeListener('_writer', onwriter)
    }

    function onappend () {
      // hack! make api in hypercore-protocol for this
      if (stream.destroyed) return
      stream.expectedFeeds += 1e9
      self._heads(function () { // will reload new feeds
        stream.expectedFeeds -= 1e9
        stream.expectedFeeds += (self._writers.length - len)
        len = self._writers.length
        if (!stream.expectedFeeds) stream.finalize()
      })
    }

    function onwriter (w) {
      if (stream.destroyed) return
      w.feed.replicate(opts)
    }
  })

  return stream
}

DB.prototype.put = function (key, value, cb) {
  var self = this

  this._lock(function (release) {
    self._put(key, value, function (err) {
      release(cb, err)
    })
  })
}

DB.prototype._put = function (key, value, cb) {
  if (!cb) cb = noop

  var self = this
  var path = hash(key, true)

  this._heads(function (err, h) {
    if (err) return cb(err)

    if (!self._localWriter) self._localWriter = self._createWriter(self.local.key, 'local')

    var feed = self._localWriter.id
    var clock = []
    var i = 0

    for (i = 0; i < self._writers.length; i++) {
      clock.push(i === feed ? 0 : self._writers[i].feed.length)
    }

    var node = {
      seq: self._writers[feed].feed.length,
      feed: feed,
      key: key,
      path: path,
      value: value,
      clock: clock,
      trie: []
    }

    if (!h.length) {
      self._writers[feed].append(node, ondone)
      return
    }

    var missing = h.length
    var error = null

    for (i = 0; i < h.length; i++) {
      self._visitPut(key, path, 0, 0, 0, h[i], h, node.trie, onput)
    }

    function onput (err) {
      if (err) error = err
      if (--missing) return

      if (error) return cb(err)

      self._writers[feed].append(node, ondone)
    }

    function ondone (err) {
      if (err) return cb(err)
      return cb(null, self._map ? self._map(node) : node)
    }
  })
}

DB.prototype.get = function (key, cb) {
  var path = hash(key, true)
  var result = []
  var self = this

  this._heads(function (err, h) {
    if (err) return cb(err)
    if (!h.length) return cb(null, null)

    var missing = h.length
    var error = null

    for (var i = 0; i < h.length; i++) {
      self._visitGet(key, path, 0, h[i], h, result, onget)
    }

    function onget (err) {
      if (err) error = err
      if (--missing) return
      if (error) return cb(error)

      if (!result.length) return cb(null, null)

      if (self._map) result = result.map(self._map)
      if (self._reduce) result = result.reduce(self._reduce)

      cb(null, result)
    }
  })
}

DB.prototype._visitPut = function (key, path, i, j, k, node, heads, trie, cb) {
  var writers = this._writers
  var self = this
  var missing = 0
  var error = null
  var vals = null
  var remoteVals = null

  for (; i < path.length; i++) {
    var val = path[i]
    var local = trie[i]
    var remote = node.trie[i] || []

    // copy old trie
    for (; j < remote.length; j++) {
      if (j === val && val !== hash.TERMINATE) continue

      if (!local) local = trie[i] = []
      vals = local[j] = local[j] || []
      remoteVals = remote[j] || []

      for (; k < remoteVals.length; k++) {
        var rval = remoteVals[k]

        if (val === hash.TERMINATE) {
          writers[rval.feed].get(rval.seq, onfilterdups)
          return
        }

        if (noDup(vals, rval)) vals.push(rval)
      }
      k = 0
    }
    j = 0

    if (node.path[i] !== val || (node.path[i] === hash.TERMINATE && node.key !== key)) {
      // trie is splitting
      if (!local) local = trie[i] = []
      vals = local[node.path[i]] = local[node.path[i]] || []
      remoteVals = remote[val]
      vals.push({feed: node.feed, seq: node.seq})

      if (!remoteVals || !remoteVals.length) return cb(null)

      missing = remoteVals.length
      error = null

      for (var l = 0; l < remoteVals.length; l++) {
        writers[remoteVals[l].feed].get(remoteVals[l].seq, onremoteval)
      }
      return
    }
  }

  cb(null)

  function onfilterdups (err, val) {
    if (err) return cb(err)
    var valPointer = {feed: val.feed, seq: val.seq}
    if (val.key !== key && noDup(vals, valPointer)) vals.push(valPointer)
    self._visitPut(key, path, i, j, k + 1, node, heads, trie, cb)
  }

  function onremoteval (err, val) {
    if (err) return onvisit(err)
    if (!updateHead(val, node, heads)) return onvisit(null)
    self._visitPut(key, path, i + 1, j, k, val, heads, trie, onvisit)
  }

  function onvisit (err) {
    if (err) error = err
    if (!--missing) cb(error)
  }
}

DB.prototype._open = function (cb) {
  var self = this
  var source = this._createFeed(this.key, 'source')

  source.on('ready', function () {
    self.source = source
    self.key = source.key
    self.discoveryKey = source.discoveryKey

    var w = self._createWriter(self.key, 'source')

    if (source.writable) {
      self.local = source
      self._localWriter = w
      return cb(null)
    }

    var local = self._createFeed(null, 'local')

    local.on('ready', function () {
      self.local = local
      cb(null)
    })
  })
}

DB.prototype._createFeed = function (key, dir) {
  if (!dir) {
    dir = key.toString('hex')
    dir = dir.slice(0, 2) + '/' + dir.slice(2)
  }

  if (key) {
    if (this.local && this.local.key && this.local.key.equals(key)) return this.local
    if (this.source && this.source.key && this.source.key.equals(key)) return this.source
  }

  var self = this
  var feed = hypercore(storage, key, {sparse: this.sparse})

  feed.on('error', onerror)
  feed.on('append', onappend)

  return feed

  function onerror (err) {
    self.emit('error', err)
  }

  function onappend () {
    self.emit('append', this)
  }

  function storage (name) {
    return self._storage(dir + '/' + name)
  }
}

DB.prototype._createWriter = function (key, dir) {
  for (var i = 0; i < this._writers.length; i++) {
    var w = this._writers[i]
    if (key && w.key.equals(key)) return w
  }

  var res = writer(this, this._createFeed(key, dir), this._writers.length)

  this._writers.push(res)
  this.emit('_writer', res)

  return res
}

DB.prototype._visitGet = function (key, path, i, node, heads, result, cb) {
  var self = this
  var writers = this._writers
  var missing = 0
  var error = null
  var trie = null
  var vals = null

  for (; i < path.length; i++) {
    if (node.path[i] === path[i]) continue

    // check trie
    trie = node.trie[i]
    if (!trie) return cb(null)

    vals = trie[path[i]]

    // not found
    if (!vals || !vals.length) return cb(null)

    missing = vals.length
    error = null

    for (var j = 0; j < vals.length; j++) {
      writers[vals[j].feed].get(vals[j].seq, onval)
    }

    return
  }

  // check for collisions
  trie = node.trie[path.length - 1]
  vals = trie && trie[hash.TERMINATE]

  pushMaybe(key, node, result)

  if (!vals || !vals.length) return cb(null)

  missing = vals.length
  error = null

  for (var k = 0; k < vals.length; k++) {
    writers[vals[k].feed].get(vals[k].seq, onpush)
  }

  function onpush (err, val) {
    if (err) error = err
    else pushMaybe(key, val, result)
    if (!--missing) cb(error)
  }

  function onval (err, val) {
    if (err) return done(err)
    if (!updateHead(val, node, heads)) return done(null)
    self._visitGet(key, path, i + 1, val, heads, result, done)
  }

  function done (err) {
    if (err) error = err
    if (!--missing) cb(error)
  }
}

function noop () {}

function updateHead (newNode, oldNode, heads) {
  var i = heads.indexOf(oldNode)
  if (i !== -1) remove(heads, i)
  if (!isHead(newNode, heads)) return false
  heads.push(newNode)
  return true
}

function isHead (node, heads) {
  for (var i = 0; i < heads.length; i++) {
    var head = heads[i]
    if (head.feed === node.feed) return false
    if (node.seq < head.clock[node.feed]) return false
  }
  return true
}

function pushMaybe (key, node, results) {
  if (node.key === key && noDup(results, node)) results.push(node)
}

function noDup (list, val) {
  for (var i = 0; i < list.length; i++) {
    if (list[i].feed === val.feed && list[i].seq === val.seq) {
      return false
    }
  }
  return true
}

function isOptions (opts) {
  return !!(opts && typeof opts !== 'string' && !Buffer.isBuffer(opts))
}

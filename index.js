var hypercore = require('hypercore')
var thunky = require('thunky')
var path = require('path')
var raf = require('random-access-file')
var protocol = require('hypercore-protocol')
var inherits = require('inherits')
var events = require('events')
var toBuffer = require('to-buffer')
var peer = require('./lib/peer')

module.exports = HyperDB

function HyperDB (storage, key, opts) {
  if (!(this instanceof HyperDB)) return new HyperDB(storage, key, opts)

  if (isOptions(key)) {
    opts = key
    key = null
  }
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  var self = this

  this.setMaxListeners(0) // can be removed once our dynamic 'add-feed' thing is removed
  this.storage = typeof storage === 'string' ? fileStorage : storage
  this.key = key ? toBuffer(key, 'hex') : null
  this.discoveryKey = null
  this.sparse = opts.sparse
  this.local = null
  this.peers = []
  this.ready = thunky(open)
  this.ready()

  function fileStorage (name) {
    return raf(name, {directory: storage})
  }

  function open (cb) {
    self._open(cb)
  }
}

inherits(HyperDB, events.EventEmitter)

HyperDB.prototype._createFeed = function (dir, key) {
  var self = this
  var feed = hypercore(storage, key, {sparse: this.sparse})

  feed.on('append', onappend)

  return feed

  function onappend () {
    self.emit('append', feed)
  }

  function storage (name) {
    return self.storage(path.join(dir, name))
  }
}

HyperDB.prototype._open = function (cb) {
  var self = this
  var source = peer(this._createFeed('source', this.key))

  source.feed.ready(function (err) {
    if (err) return cb(err)

    self.key = source.feed.key
    self.discoveryKey = source.feed.discoveryKey
    self.peers.push(source)
    self.emit('add-feed', source.feed)

    if (source.feed.writable) self.local = source
    if (self.local) return onme()

    self.local = peer(self._createFeed('local'))
    self.local.feed.ready(onme)
  })

  function done () {
    var missing = self.peers.length
    var error = null

    for (var i = 0; i < self.peers.length; i++) self.peers[i].feed.ready(onready)

    function onready (err) {
      if (err) error = err
      if (--missing) return
      cb(null)
    }
  }

  function onme (err) {
    if (err) return cb(err)
    if (self.local.length > 0) return update(null)
    self.local.header({type: 'hyperdb', version: 0}, update)
  }

  function update (err) {
    if (err) return cb(err)
    self._update({cached: true}, done)
  }
}

HyperDB.prototype.put = function (key, value, cb) {
  if (typeof value === 'string') value = toBuffer(value)
  if (!cb) cb = noop

  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    self._heads(function (err, heads, set) {
      if (err) return cb(err)
      if (!set) return self._init(key, value, cb)
      self._put(heads, key, value, cb)
    })
  })
}

HyperDB.prototype._init = function (key, value, cb) {
  var node = {
    heads: [],
    key: key,
    value: value
  }

  this.local.append(node, cb)
}

HyperDB.prototype._put = function (heads, key, value, cb) {
  console.log('_put')
  console.log(heads)
}

HyperDB.prototype._heads = function (cb) {
  var heads = []
  var missing = this.peers.length
  var error = null
  var set = 0

  this.peers.forEach(function (peer, i) {
    peer.head(function (err, head) {
      if (err) error = err

      if (!err) {
        if (head) set++
        heads[i] = head
      }

      if (--missing) return
      cb(error, heads, set)
    })
  })
}

HyperDB.prototype._update = function (opts, cb) {
  cb(null)
}

function noop () {}

function isOptions (opts) {
  return !!(opts && typeof opts !== 'string' && !Buffer.isBuffer(opts))
}


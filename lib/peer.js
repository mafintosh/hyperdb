var alru = require('array-lru')
var messages = require('./messages')

module.exports = Peer

function Peer (feed) {
  if (!(this instanceof Peer)) return new Peer(feed)

  this.id = -1
  this.owner = false
  this.writer = false
  this.cache = alru(65536)
  this.feed = feed
  this.key = feed.key
}

Peer.prototype.feeds = function (opts, cb) {
  if (typeof opts === 'function') return this.feeds(null, opts)

  this.head(opts, function (err, head) {
    if (err) return cb(err)
    if (head.feeds.length) return cb(null, head.feeds)
    self.get(head.pointer, cb)
  })
}

Peer.prototype.head = function (opts, cb) {
  if (typeof opts === 'function') return this.head(null, opts)

  var self = this
  this.feed.ready(function (err) {
    if (err) return cb(err)
    self._head(opts, cb)
  })
}

Peer.prototype._head = function (opts, cb) {
  var cached = !!(opts && opts.cached)
  var len = cached ? this.remoteLength() : this.feed.length

  if (len === 0) {
    if (!cached) return retryHead(this, cb)
  }
  if (len < 2) {
    return cb(null, null, -1)
  }

  this.get(len - 1, cb)
}

Peer.prototype.get = function (seq, cb) {
  var self = this
  var node = this.cache.get(seq)

  if (node) return process.nextTick(cb, null, node)
  this._get(seq, cb)
}

Peer.prototype._get = function (seq, cb) {
  var self = this

  this.feed.get(seq, function (err, val) {
    if (err) return cb(err)
    var node = messages.NodeWrap.decode(val)
    node.seq = seq
    self.cache.set(seq, node)
    cb(null, node)
  })
}

Peer.prototype.header = function (header, cb) {
  this.feed.append(messages.Header.encode(header), cb)
}

Peer.prototype.append = function (node, cb) {
  this.feed.append(messages.Node.encode(node), cb)
}

Peer.prototype.remoteLength = function () {
  var len = this.feed.length
  for (var i = 0; i < this.feed.peers.length; i++) {
    len = Math.max(len, this.feed.peers[i].remoteLength)
  }
  return len
}

function retryHead (self, cb) {
  self.feed.update(function (err) {
    if (err) return cb(err)
    self.head(cb)
  })
}

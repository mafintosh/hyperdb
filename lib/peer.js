var alru = require('array-lru')

module.exports = Peer

function Peer (feed) {
  if (!(this instanceof Peer)) return new Peer(feed)

  this.feed = feed
  this.cache = alru(65536)
}

Peer.prototype.get = function (index, cb) {
  var self = this

  var node = this.cache.get(index)
  if (node) return process.nextTick(cb, null, node)

  this.feed.get(index, function (err, node) {
    if (err) return cb(err)
    self.cache.set(index, node)
    cb(null, node)
  })
}

Peer.prototype.head = function (cb) {
  var len = this._remoteLength()
  if (len < 2) return cb(null, null)
  this.get(len - 1, cb)
}

Peer.prototype._remoteLength = function () {
  var len = this.feed.length

  for (var i = 0; i < this.feed.peers.length; i++) {
    len = Math.max(this.feed.peers[i].remoteLength, len)
  }

  return len
}

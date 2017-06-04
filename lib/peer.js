var alru = require('array-lru')
// var toBuffer = require('to-buffer')

module.exports = Peer

function Peer (feed) {
  if (!(this instanceof Peer)) return new Peer(feed)

  this.feed = feed
  this.cache = alru(65536)
}

// Peer.prototype.nextIndex = function () {
//   return this.feed.length < 2 ? 1 : this.feed.length
// }

// Peer.prototype.append = function (data, cb) {
//   var buf = toBuffer(JSON.stringify(data) + '\n')
//   if (this.feed.length === 0) {
//     var enc = toBuffer(JSON.stringify({type: 'hyperdb', version: 0}))
//     this.feed.append([enc, buf], cb)
//   } else {
//     this.feed.append(buf, cb)
//   }
// }

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

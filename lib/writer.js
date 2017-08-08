var alru = require('array-lru')
var hash = require('./hash')
var encoding = require('./encoding')

module.exports = Writer

function Writer (feed, id) {
  if (!(this instanceof Writer)) return new Writer(feed, id)
  this.id = id
  this.feed = feed
  this.cache = alru(8192)
}

Writer.prototype.get = function (seq, cb) {
  var self = this

  var node = this.cache.get(seq)
  if (node) return process.nextTick(cb, null, node)

  this.feed.get(seq, function (err, val) {
    if (err) return cb(err)
    node = encoding.decode(val)
    node.feed = self.id
    node.seq = seq
    node.path = hash(node.key, true)
    self.cache.set(seq, node)
    cb(null, node)
  })
}

Writer.prototype.head = function (cb) {
  if (!this.feed.length) return process.nextTick(cb, null, null)
  this.get(this.feed.length - 1, cb)
}

Writer.prototype.append = function (node, cb) {
  this.feed.append(encoding.encode(node), cb)
}

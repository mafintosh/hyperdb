var alru = require('array-lru')

module.exports = Writer

function Writer (feed) {
  if (!(this instanceof Writer)) return new Writer(feed)
  this.feed = feed
  this.cache = alru(65536)
}

Writer.prototype.get = function (seq, cb) {
  var self = this

  var node = this.cache.get(seq)
  if (node) return process.nextTick(cb, null, node)

  this.feed.get(seq, function (err, val) {
    if (err) return cb(err)
    self.cache.set(seq, val)
    cb(null, val)
  })
}

Writer.prototype.head = function (cb) {
  if (!this.feed.length) return process.nextTick(cb, null, null)
  this.get(this.feed.length - 1, cb)
}

Writer.prototype.append = function (node, cb) {
  this.feed.append(node, cb)
}

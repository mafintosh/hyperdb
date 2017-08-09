var alru = require('array-lru')
var hash = require('./hash')
var trie = require('./trie')
var messages = require('./messages')

module.exports = Writer

function Writer (feed, id, codec) {
  if (!(this instanceof Writer)) return new Writer(feed, id, codec)

  this.id = id
  this.feed = feed
  this.cache = alru(8192)
  this.codec = codec
}

Writer.prototype.get = function (seq, cb) {
  var self = this
  var node = this.cache.get(seq)
  if (node) return process.nextTick(cb, null, node)

  this.feed.get(seq, function (err, val) {
    if (err) return cb(err)
    cb(null, self._decode(val, seq))
  })
}

Writer.prototype.head = function (cb) {
  if (!this.feed.length) return process.nextTick(cb, null, null)
  this.get(this.feed.length - 1, cb)
}

Writer.prototype.append = function (node, cb) {
  var buf = messages.Node.encode({
    key: node.key,
    value: this.codec ? this.codec.encode(node.value) : node.value,
    heads: node.heads,
    trie: trie.encode(node.trie)
  })

  this.feed.append(buf, cb)
}

Writer.prototype._decode = function (val, seq) {
  var node = messages.Node.decode(val)
  node.feed = this.id
  node.seq = seq
  node.path = hash(node.key, true)
  node.trie = trie.decode(node.trie)
  if (this.codec) node.value = this.codec.decode(node.value)
  this.cache.set(seq, node)
  return node
}

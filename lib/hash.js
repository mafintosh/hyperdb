var sodium = require('sodium-universal')

var KEY = Buffer.alloc(16)
var OUT = Buffer.alloc(8)

hash.TERMINATE = 5
hash.SEPARATE = 4

module.exports = function getHash (opts) {
  if (!opts) return hash
  return opts.lexint ? lexint : hash
}

function lexint (keys, terminate) {
  if (typeof keys === 'string') keys = split(keys)
  if (!keys.length) return []

  var lengths = keys.map(k => k.length)
  var totalSize = lengths.reduce(sum, 0) * 4

  var all = new Array(totalSize + keys.length + (terminate ? 1 : 0))

  var offset = 0
  for (var i = 0; i < keys.length; i++) {
    var buf = Buffer.from(keys[i])
    expandComponent(buf, all, offset)
    offset += lengths[i] * 4
    all[offset++] = hash.SEPARATE
  }

  if (terminate) all[all.length - 1] = hash.TERMINATE

  return all
}

function hash (keys, terminate) {
  if (typeof keys === 'string') keys = split(keys)
  if (!keys.length) return []

  var all = new Array(keys.length * 32  + keys.length + (terminate ? 1 : 0))

  var offset = 0
  for (var i = 0; i < keys.length; i++) {
    sodium.crypto_shorthash(OUT, Buffer.from(keys[i]), KEY)
    expandComponent(OUT, all, i * 32)
    offset += 32
    all[offset++] = hash.SEPARATE
  }

  if (terminate) all[all.length - 1] = hash.TERMINATE

  return all
}

function expandComponent (next, out, offset) {
  for (var i = 0; i < next.length; i++) {
    var n = next[i]

    for (var j = 3; j >= 0; j--) {
      var r = n & 3
      out[offset + 4 * i + j] = r
      n -= r
      n /= 4
    }
  }
}

function sum (s, n) { return s + n }

function split (key) {
  var list = key.split('/')
  if (list[0] === '') list.shift()
  if (list[list.length - 1] === '') list.pop()
  return list
}

var sodium = require('sodium-universal')

var KEY = Buffer.alloc(16)
var OUT = Buffer.alloc(8)

hash.TERMINATE = 4
hash.LENGTH = 32

module.exports = hash

function hash (keys, terminate) {
  if (typeof keys === 'string') keys = split(keys)

  var all = new Array(keys.length * 32 + (terminate ? 1 : 0))

  for (var i = 0; i < keys.length; i++) {
    sodium.crypto_shorthash(OUT, Buffer.from(keys[i]), KEY)
    expandHash(OUT, all, i * 32)
  }

  if (terminate) all[all.length - 1] = 4

  return all
}

function expandHash (next, out, offset) {
  for (var i = 0; i < next.length; i++) {
    var n = next[i]

    for (var j = 0; j < 4; j++) {
      var r = n & 3
      out[offset++] = r
      n -= r
      n /= 4
    }
  }
}

function split (key) {
  var list = key.split('/')
  if (list[0] === '') list.shift()
  if (list[list.length - 1] === '') list.pop()
  return list
}

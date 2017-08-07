var sodium = require('sodium-native')
var toBuffer = require('to-buffer')

var KEY = Buffer.alloc(16)
var OUT = Buffer.alloc(8)

module.exports = function (hash, length) {
  if (!hash) hash = defaultHash
  if (!length) length = 8

  var expandedLength = length * 4

  return function (keys) {
    if (typeof keys === 'string') keys = split(keys)
    var all = new Array(keys.length * expandedLength)
    for (var i = 0; i < keys.length; i++) {
      expandHash(hash(toBuffer(keys[i])), all, i * expandedLength)
    }
    return all
  }
}

function defaultHash (key) {
  sodium.crypto_shorthash(OUT, key, KEY)
  return OUT
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

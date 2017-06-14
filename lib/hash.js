var sodium = require('sodium-native')
var alloc = require('buffer-alloc')
var allocUnsafe = require('buffer-alloc-unsafe')

var KEY = Buffer.alloc(16)
var OUT = Buffer.allocUnsafe(8)

module.exports = function (keys) {
  var all = new Array(keys.length)
  for (var i = 0; i < all.length; i++) all[i] = hash(keys[i])
  return all
}

function hash (key) {
  sodium.crypto_shorthash(OUT, typeof key === 'string' ? new Buffer(key) : key, KEY)

  var hash = new Array(32)

  for (var i = 0; i < 8; i++) {
    var n = OUT[i]

    for (var j = 0; j < 4; j++) {
      var r = n & 3
      hash[4 * i + j] = r
      n -= r
      n /= 4
    }
  }

  return hash
}

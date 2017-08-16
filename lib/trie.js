var varint = require('varint')

var buf = Buffer.allocUnsafe(512 * 1024)
var ptr = 0

exports.encode = encode
exports.decode = decode

function decode (buf, map) { // decodes and inflates the trie
  var trie = []
  var ptr = 0
  var delta = 0

  while (ptr < buf.length) {
    delta += varint.decode(buf, ptr)
    ptr += varint.decode.bytes

    var cnt = 0
    var val = 0
    var bucket = trie[delta] = []
    var innerBitfield = varint.decode(buf, ptr)
    ptr += varint.decode.bytes

    while (innerBitfield > 0) {
      var next = innerBitfield & 1
      innerBitfield >>= 1

      if (!next) {
        if (cnt) bucket[val] = new Array(cnt)
        val++
        cnt = 0
      } else {
        cnt++
      }
    }

    if (cnt) bucket[val] = new Array(cnt)

    for (var i = 0; i < bucket.length; i++) {
      var vals = bucket[i]
      if (!vals) continue
      for (var j = 0; j < vals.length; j++) {
        var feed = varint.decode(buf, ptr)
        ptr += varint.decode.bytes
        var seq = varint.decode(buf, ptr)
        ptr += varint.decode.bytes
        vals[j] = {feed: map[feed], seq: seq}
      }
    }
  }

  return trie
}

function encodeVarint (n) {
  varint.encode(n, buf, ptr)
  ptr += varint.encode.bytes
}

function encode (trie, map) { // encodes and deflates the trie
  // trie          -> [trie-position (usually 0-32)]
  // trie-position -> [hash-value (0-4)]
  // hash-value    -> [{feed: feed, seq: seq}]

  if (buf.length - ptr < 65536) {
    buf = Buffer.allocUnsafe(buf.length)
    ptr = 0
  }

  var delta = 0
  var prev = ptr

  for (var i = 0; i < trie.length; i++) {
    var bucket = trie[i]
    if (!bucket || !bucket.length) continue

    encodeVarint(i - delta)
    encodeBitfield(bucket)
    delta = i

    for (var j = 0; j < bucket.length; j++) {
      var vals = bucket[j]
      if (!vals || !vals.length) continue
      for (var k = 0; k < vals.length; k++) {
        var v = vals[k]
        encodeVarint(map[v.feed])
        encodeVarint(v.seq)
      }
    }
  }

  return buf.slice(prev, ptr)
}

function encodeBitfield (bucket) {
  // TODO there is an edge case where innerBitfield becomes
  // too large of a number to be a bitfield (if there is *lots* of conflicts)
  // this most likely wont happen but we should guard against it anyhow

  var innerBitfield = 0
  var innerNext = 1

  for (var i = 0; i < bucket.length; i++) {
    var vals = bucket[i]
    if (vals && vals.length) {
      for (var j = 0; j < vals.length; j++) {
        innerBitfield |= innerNext
        innerNext <<= 1
      }
    }
    innerNext <<= 1
  }

  encodeVarint(innerBitfield)
}

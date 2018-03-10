var varint = require('varint')

var buf = Buffer.allocUnsafe(512 * 1024)
var offset = 0

exports.encode = encode
exports.decode = decode

// encoding: i+bitfield+vals+...
// val = (feed << 1)+more?,seq

function encode (trie, map) {
  if (buf.length - offset < 65536) {
    offset = 0
    buf = Buffer.allocUnsafe(buf.length)
  }

  var oldOffset = offset
  for (var i = 0; i < trie.length; i++) {
    if (!trie[i]) continue
    varint.encode(i, buf, offset)
    offset += varint.encode.bytes
    offset = encodeBucket(trie[i], map, buf, offset)
  }

  return buf.slice(oldOffset, offset)
}

function encodeBucket (bucket, map, buf, offset) {
  var i
  var bits = 0
  var bit = 1

  for (i = 0; i < bucket.length; i++) {
    if (bucket[i] && bucket[i].length) bits |= bit
    bit *= 2
  }

  varint.encode(bits, buf, offset)
  offset += varint.encode.bytes

  for (i = 0; i < bucket.length; i++) {
    var vals = bucket[i]
    if (!vals) continue

    for (var j = 0; j < vals.length; j++) {
      offset = encodeValue(vals[j], j < vals.length - 1, map, buf, offset)
    }
  }

  return offset
}

function encodeValue (ptr, more, map, buf, offset) {
  varint.encode(map[ptr.feed] * 2 + (more ? 1 : 0), buf, offset)
  offset += varint.encode.bytes
  varint.encode(ptr.seq, buf, offset)
  offset += varint.encode.bytes
  return offset
}

function decode (buf, map) {
  var trie = []
  var offset = 0

  while (offset < buf.length) {
    var i = varint.decode(buf, offset)
    offset += varint.decode.bytes
    trie[i] = []
    offset = decodeBucket(buf, offset, trie[i], map)
  }

  return trie
}

function decodeBucket (buf, offset, bucket, map) {
  var i = 0
  var bits = varint.decode(buf, offset)
  offset += varint.decode.bytes

  while (bits) {
    if (bits & 1) {
      bucket[i] = []
      offset = decodeValues(buf, offset, bucket[i], map)
      bits = (bits - 1) / 2
    } else {
      bits /= 2
    }
    i++
  }

  return offset
}

function decodeValues (buf, offset, values, map) {
  var more = 1
  while (more) {
    var feed = varint.decode(buf, offset)
    offset += varint.decode.bytes
    var seq = varint.decode(buf, offset)
    offset += varint.decode.bytes
    more = feed & 1
    feed = (feed - more) / 2
    if (feed < map.length) feed = map[feed]
    values.push({feed, seq})
  }
  return offset
}

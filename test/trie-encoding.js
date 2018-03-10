var tape = require('tape')
var enc = require('../lib/trie-encoding')

tape('encode trie', function (t) {
  t.same(enc.encode([], []), Buffer.alloc(0), 'empty trie')
  t.same(enc.encode([[[{feed: 0, seq: 0}]]], [0]), Buffer.from([0, 1, 0, 0]))
  t.same(enc.encode(set(1, [[{feed: 0, seq: 0}]]), [0]), Buffer.from([1, 1, 0, 0]))
  t.same(enc.encode(set(1, [[{feed: 0, seq: 0}]]), [1]), Buffer.from([1, 1, 2, 0]))
  t.same(enc.encode(set(1, [[{feed: 0, seq: 0}, {feed: 0, seq: 1}]]), [0]), Buffer.from([1, 1, 1, 0, 0, 1]))
  t.same(enc.encode(set(1, set(1, [{feed: 0, seq: 0}, {feed: 0, seq: 1}])), [0]), Buffer.from([1, 2, 1, 0, 0, 1]))
  t.end()
})

tape('decode trie', function (t) {
  t.same(enc.decode(Buffer.alloc(0), []), [], 'empty trie')
  t.same(enc.decode(Buffer.from([0, 1, 0, 0]), [0]), [[[{feed: 0, seq: 0}]]])
  t.same(enc.decode(Buffer.from([1, 1, 0, 0]), [0]), set(1, [[{feed: 0, seq: 0}]]))
  t.same(enc.decode(Buffer.from([1, 1, 2, 0]), [1, 0]), set(1, [[{feed: 0, seq: 0}]]))
  t.same(enc.decode(Buffer.from([1, 1, 1, 0, 0, 1]), [0]), set(1, [[{feed: 0, seq: 0}, {feed: 0, seq: 1}]]))
  t.same(enc.decode(Buffer.from([1, 2, 1, 0, 0, 1]), [0]), set(1, set(1, [{feed: 0, seq: 0}, {feed: 0, seq: 1}])))
  t.end()
})

tape('encode and decode complex trie', function (t) {
  var target = set(32, set(2, [{feed: 0, seq: 0}], [{feed: 0, seq: 2}], [{feed: 0, seq: 0}]))
  var clone = enc.decode(enc.encode(target, [0]), [0])

  t.same(clone, target)
  t.end()
})

function set (i, val) {
  var arr = []
  arr[i] = val
  return arr
}

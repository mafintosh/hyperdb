var tape = require('tape')
var create = require('./helpers/create')

tape('onlookup hook', function (t) {
  var db = create.one()
  var batch = []
  var path = []

  for (var i = 0; i < 200; i++) {
    batch.push({type: 'put', key: '' + i, value: '' + i})
  }

  db.batch(batch, function (err) {
    t.error(err, 'no error')
    db.get('0', {onlookup}, function (err, node) {
      t.error(err, 'no error')
      db._getAllPointers(path, false, function (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes[0].seq, db.feeds[0].length - 1, 'first is head')
        for (var i = 1; i < nodes.length; i++) {
          t.ok(inTrie(nodes[i - 1], nodes[i]), 'in trie')
        }
        t.same(nodes[nodes.length - 1].seq, node.seq, 'last node is the found one')
        t.end()
      })
    })

    function inTrie (node, ptr) {
      return node.trie.some(function (bucket) {
        if (!bucket) return false
        return bucket.some(function (values) {
          if (!values) return false
          return values.some(function (val) {
            return val.feed === ptr.feed && val.seq === ptr.seq
          })
        })
      })
    }

    function onlookup (ptr) {
      path.push(ptr)
    }
  })
})

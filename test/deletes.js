var tape = require('tape')
var create = require('./helpers/create')
var run = require('./helpers/run')

tape('basic delete', function (t) {
  var db = create.one()

  db.put('hello', 'world', function () {
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.value, 'world')
      db.del('hello', function (err) {
        t.error(err, 'no error')
        db.get('hello', function (err, node) {
          t.error(err, 'no error')
          t.ok(!node, 'was deleted')
          t.end()
        })
      })
    })
  })
})

tape('delete one in many', function (t) {
  t.plan(1 + 2 + 2)

  var db = create.one()
  var keys = []

  for (var i = 0; i < 50; i++) {
    keys.push('' + i)
  }

  run(
    keys.map(k => cb => db.put(k, k, cb)),
    cb => db.del('42', cb),
    done
  )

  function done (err) {
    t.error(err, 'no error')
    db.get('42', function (err, node) {
      t.error(err, 'no error')
      t.ok(!node, 'was deleted')
    })
    db.get('43', function (err, node) {
      t.error(err, 'no erro')
      t.same(node.value, '43')
    })
  }
})

tape('delete one in many (iteration)', function (t) {
  var db = create.one()
  var keys = []

  for (var i = 0; i < 50; i++) {
    keys.push('' + i)
  }

  run(
    keys.map(k => cb => db.put(k, k, cb)),
    cb => db.del('42', cb),
    done
  )

  function done (err) {
    t.error(err, 'no error')

    var ite = db.iterator()
    var actual = []

    ite.next(function loop (err, node) {
      if (err) return t.error(err, 'no error')

      if (!node) {
        var expected = keys.slice(0, 42).concat(keys.slice(43))
        t.same(actual.sort(), expected.sort(), 'all except deleted one')
        t.end()
        return
      }

      actual.push(node.value)
      ite.next(loop)
    })
  }
})

tape('delete marks node as deleted', function (t) {
  var db = create.one()
  var expected = [{key: 'hello', value: 'world', deleted: false}, {key: 'hello', value: null, deleted: true}]

  db.put('hello', 'world', function () {
    db.del('hello', function () {
      db.createHistoryStream()
        .on('data', function (data) {
          t.same({key: data.key, value: data.value, deleted: data.deleted}, expected.shift())
        })
        .on('end', function () {
          t.same(expected.length, 0)
          t.end()
        })
    })
  })
})

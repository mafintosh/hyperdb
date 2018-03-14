var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var put = require('./helpers/put')

function toKeyValuePairs (value) {
  return (k) => ({ key: k, value: value || k })
}

function indexWithKey (key) {
  return v => v.key === key
}

tape('basic readStream', { timeout: 1000 }, function (t) {
  var db = create.one()
  var vals = ['foo', 'foo/a', 'foo/b', 'a', 'bar/a', 'foo/abc', 'foo/b', 'bar/b', 'foo/bar', 'foo/a/b']
  var expected = ['foo/a', 'foo/abc', 'foo/b', 'foo/bar', 'foo/a/b']
  put(db, vals, validate)

  function validate (err) {
    t.error(err, 'no error')
    var reader = db.createReadStream('foo/', {gt: true})
    reader.on('data', (data) => {
      var index = expected.indexOf(data.key)
      t.ok(index !== -1, 'key is expected')
      if (index >= 0) expected.splice(index, 1)
    })
    reader.on('end', () => {
      t.equals(expected.length, 0)
      t.end()
    })
    reader.on('error', (err) => {
      t.fail(err.message)
      t.end()
    })
  }
})

tape('basic readStream (again)', { timeout: 1000 }, function (t) {
  var db = create.one()
  var vals = ['foo/a', 'foo/abc', 'foo/a/b']
  var expected = ['foo/a', 'foo/a/b']
  put(db, vals, validate)

  function validate (err) {
    t.error(err, 'no error')
    var reader = db.createReadStream('foo/a')
    reader.on('data', (data) => {
      var index = expected.indexOf(data.key)
      t.ok(index !== -1, 'key is expected')
      if (index >= 0) expected.splice(index, 1)
    })
    reader.on('end', () => {
      t.equals(expected.length, 0)
      t.end()
    })
    reader.on('error', (err) => {
      t.fail(err.message)
      t.end()
    })
  }
})

tape('readStream with two feeds', { timeout: 1000 }, function (t) {
  create.two((a, b) => {
    var aValues = ['b/a', 'a/b/c', 'b/c', 'b/c/d'].map(toKeyValuePairs('A'))
    var bValues = ['a/b', 'a/b/c', 'b/c/d', 'b/c'].map(toKeyValuePairs('B'))
    put(a, aValues, (err) => {
      t.error(err, 'no error')
      replicate(a, b, () => {
        put(b, bValues, (err) => {
          t.error(err, 'no error')
          replicate(a, b, validate)
        })
      })
    })
    function validate (err) {
      t.error(err, 'no error')
      var reader = a.createReadStream('b/')
      var expected = [
        { key: 'b/c/d', value: 'B' },
        { key: 'b/c', value: 'B' },
        { key: 'b/a', value: 'A' }
      ]
      reader.on('data', (nodes) => {
        t.equals(nodes.length, 1)
        const index = expected.findIndex(indexWithKey(nodes[0].key))
        t.ok(index !== -1, 'key is expected')
        if (index >= 0) {
          var found = expected.splice(index, 1)
          t.same(found[0].value, nodes[0].value)
        }
      })
      reader.on('end', () => {
        t.ok(expected.length === 0, 'received all expected')
        t.pass('stream ended ok')
        t.end()
      })
      reader.on('error', (err) => {
        t.fail(err.message)
        t.end()
      })
    }
  })
})

tape('readStream with two feeds (again)', { timeout: 1000 }, function (t) {
  var aValues = ['/a/a', '/a/b', '/a/c'].map(toKeyValuePairs('A'))
  var bValues = ['/b/a', '/b/b', '/b/c', '/a/a', '/a/b', '/a/c'].map(toKeyValuePairs('B'))
  create.two((a, b) => {
    put(a, aValues, (err) => {
      t.error(err)
      replicate(a, b, () => {
        put(b, bValues, (err) => {
          t.error(err)
          replicate(a, b, validate)
        })
      })
    })
    function validate () {
      var reader = b.createReadStream('/')
      var expected = ['b/a', 'b/b', 'b/c', 'a/a', 'a/b', 'a/c']
      reader.on('data', (data) => {
        t.equals(data.length, 1)
        var index = expected.indexOf(data[0].key)
        t.ok(index !== -1, 'key is expected')
        t.same(data[0].value, 'B')
        if (index >= 0) expected.splice(index, 1)
      })
      reader.on('end', () => {
        t.ok(expected.length === 0, 'received all expected')
        t.pass('stream ended ok')
        t.end()
      })
      reader.on('error', (err) => {
        t.fail(err.message)
        t.end()
      })
    }
  })
})

tape('readStream with conflicting feeds', { timeout: 2000 }, function (t) {
  var conflictingKeys = ['c/a', 'c/b', 'c/c', 'c/d']
  create.two((a, b) => {
    put(a, ['a/a', 'a/b', 'a/c'].map(toKeyValuePairs('A')), (err) => {
      t.error(err)
      replicate(a, b, () => {
        put(b, ['b/a', 'b/b', 'b/c'].map(toKeyValuePairs('B')), (err) => {
          t.error(err)
          replicate(a, b, (err) => {
            t.error(err)
            put(a, conflictingKeys.map(toKeyValuePairs('A')), (err) => {
              t.error(err)
              put(b, conflictingKeys.reverse().map(toKeyValuePairs('B')), (err) => {
                t.error(err)
                replicate(a, b, validate)
              })
            })
          })
        })
      })
    })
    function validate () {
      var expected = ['a/a', 'a/b', 'a/c', 'b/a', 'b/b', 'b/c', 'c/b', 'c/c', 'c/a', 'c/d']
      var reader = a.createReadStream('/')
      reader.on('data', (data) => {
        var isConflicting = conflictingKeys.indexOf(data[0].key) >= 0
        if (isConflicting) {
          t.equals(data.length, 2)
        } else {
          t.equals(data.length, 1)
        }
        var index = expected.indexOf(data[0].key)

        t.ok(index !== -1, 'key is expected')
        if (index >= 0) expected.splice(index, 1)
      })
      reader.on('end', () => {
        t.ok(expected.length === 0, 'received all expected')
        t.pass('stream ended ok')
        t.end()
      })
      reader.on('error', (err) => {
        t.fail(err.message)
        t.end()
      })
    }
  })
})

var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var populate = require('./helpers/populate')

tape('basic readStream', { timeout: 1000 }, function (t) {
  var db = create.one()
  var vals = ['foo', 'foo/a', 'foo/b', 'aa', 'bb', 'c', 'bar/baz', 'foo/abc', 'foo/b', 'bar/cat', 'foo/bar', 'bar/cat', 'something']
  var expected = ['foo/a', 'foo/abc', 'foo/b', 'foo/bar']
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  populate(db, vals, 0, validate)

  function validate (err) {
    t.error(err, 'no error')
    var reader = db.createReadStream('foo/')
    var previousValue = 100000000
    reader.on('data', (data) => {
      t.ok(previousValue > data.value)
      t.same(data.key, expected.pop())
      previousValue = data.value
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
    var aValues = ['a/b', 'a/b/c', 'b/c', 'b/c/d']
    var bValues = ['a/b', 'a/b/c', 'b/c/d', 'b/c']
    populate(a, aValues, 0, (err) => {
      t.error(err, 'no error')
      replicate(a, b, () => {
        populate(b, bValues, 4, (err) => {
          t.error(err, 'no error')
          replicate(a, b, validate)
        })
      })
    })
    function validate (err) {
      t.error(err, 'no error')
      var reader = a.createReadStream('b/')
      var expectedValues = [7, 6]
      reader.on('data', (nodes) => {
        // console.log('data---', nodes[0].key, nodes.map(v => v.value))
        t.equals(nodes.length, 1)
        t.equals(nodes[0].value, expectedValues.shift())
      })
      reader.on('end', () => {
        t.ok(expectedValues.length === 0)
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
  create.two((a, b) => {
    populate(a, ['/a/a', '/a/b', '/a/c'], 0, (err) => {
      t.error(err)
      replicate(a, b, () => {
        populate(b, ['/b/a', '/b/b', '/b/c', '/a/a', '/a/b', '/a/c'], 3, (err) => {
          t.error(err)
          replicate(a, b, validate)
        })
      })
    })
    function validate () {
      var reader = b.createReadStream('/')
      var previousValue = 10000
      var total = 0
      reader.on('data', (nodes) => {
        // console.log('data ->', nodes.map(n => n.key + ' ' + n.value))
        t.ok(previousValue > nodes[nodes.length - 1].value)
        previousValue = nodes[nodes.length - 1].value
        total++
      })
      reader.on('end', () => {
        t.equals(total, 6)
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
  var conflictingKeys = ['/c/a', '/c/b', '/c/c', '/c/d']
  var expectedKeys = ['/a/a', '/a/b', '/a/c', '/b/a', '/b/b', '/b/c', '/c/b', '/c/c', '/c/a', '/c/d'].reverse()
  create.two((a, b) => {
    populate(a, ['/a/a', '/a/b', '/a/c'], 0, (err) => {
      t.error(err)
      replicate(a, b, () => {
        populate(b, ['/b/a', '/b/b', '/b/c'], 3, (err) => {
          t.error(err)
          replicate(a, b, (err) => {
            t.error(err)
            populate(a, conflictingKeys, 6, (err) => {
              t.error(err)
              populate(b, conflictingKeys.reverse(), 6 + conflictingKeys.length, (err) => {
                t.error(err)
                replicate(a, b, validate)
              })
            })
          })
        })
      })
    })
    function validate () {
      var reader = a.createReadStream('/')
      reader.on('data', (data) => {
        // console.log(data[0].key, data.map((d) => `${d.feed},${d.clock.reduce((p, v) => p + v, d.seq)}:${d.value}`))
        t.same(data[0].key, expectedKeys.shift())
      })
      reader.on('end', () => {
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

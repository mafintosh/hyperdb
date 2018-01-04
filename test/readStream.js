var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

function populate (db, vals, offset, cb) {
  var writer = db.createWriteStream()
  // writer.write(vals.map((v, i) => ({
  //   type: 'put',
  //   key: v,
  //   value: (offset || 0) + i
  // })))
  vals.forEach((v, i) => writer.write({
    type: 'put',
    key: v,
    value: (offset || 0) + i
  }))
  writer.end((err) => {
    cb(err)
  })
}

tape('basic readStream', { timeout: 1000 }, function (t) {
  var db = create.one()
  var vals = ['foo', 'foo/a', 'foo/b', 'aa', 'bb', 'c', 'bar/baz', 'foo/abc', 'foo/b', 'bar/cat', 'foo/bar', 'bar/cat', 'something']
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  populate(db, vals, 0, validate)

  function validate (err) {
    t.error(err, 'no error')
    var reader = db.createReadStream('foo/')
    var fooCount = 0
    reader.on('data', (data) => {
      if (data.key === 'foo/b') fooCount++
      // console.log('data,', data.key, '----', data.value)
    })
    reader.on('end', () => {
      t.equals(fooCount, 1)
      t.pass('stream ended ok')
      t.end()
    })
    reader.on('error', (err) => {
      console.log('Something went wrong', err)
      t.fail(err.message)
      t.end()
    })
  }
})

tape('readStream with two feeds', { timeout: 1000 }, function (t) {
  create.two((a, b) => {
    var aValues = ['a/b', 'a/b/c', 'b/c', 'b/c/d']
    populate(a, aValues, 0, (err) => {
      t.error(err, 'no error')
      replicate(a, b, () => {
        var bValues = ['a/b', 'a/b/c', 'b/c', 'b/c/d']
        populate(b, bValues, 4, validate)
      })
    })
    function validate (err) {
      t.error(err, 'no error')
      var reader = b.createReadStream('b/')
      reader.on('data', (data) => {
        // console.log('data---', data.key, data.value)
        t.equals(data.value, expectedValues.shift())
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
      t.error(err)
      replicate(a, b, () => {
          t.error(err)
          replicate(a, b, validate)
        })
      })
    })
    function validate () {
      reader.on('data', (data) => {
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

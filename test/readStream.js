var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('basic readStream', { timeout: 1000 }, function (t) {
  // t.plan(2)
  var db = create.one()
  var writer = db.createWriteStream()
  var vals = ['foo', 'foo/a', 'foo/b', 'aa', 'bb', 'c', 'bar/baz', 'foo/abc', 'foo/b', 'bar/cat', 'foo/bar', 'bar/cat', 'aa', 'bb', 'c']
  vals = vals.concat(vals)
  vals = vals.concat(vals)
  // vals = vals.concat(vals)
  // vals = vals.concat(vals)
  // vals = vals.concat(vals)
  // vals = vals.concat(vals)
  // var vals = ['a/b', 'a/c']
  writer.write(vals.map((v, i) => ({
    type: 'put',
    key: v,
    value: i
  })))

  writer.end((err) => {
    t.error(err, 'no error')
    var reader = db.createReadStream('bar/')
    reader.on('data', (data) => {
      console.log('data,', data.key, '----', data.value)
    })
    reader.on('end', () => {
      t.pass('stream ended ok')
      t.end()
    })
    reader.on('error', (err) => {
      console.log('Something went wrong', err)
      t.fail(err.message)
      t.end()
    })
  })
})

tape.only('readStream with two feeds', { timeout: 1000 }, function (t) {
  create.two((a, b) => {
    var aValues = ['a/b', 'a/b/c', 'b/c', 'b/c/d']
    var aWriter = a.createWriteStream()
    aWriter.write(aValues.map((v, i) => ({
      type: 'put',
      key: v,
      value: i
    })))
    aWriter.end((err) => {
      t.error(err, 'no error')
      replicate(a, b, writeToB)
    })
    function writeToB () {
      var bValues = ['a/b', 'a/b/c', 'b/c', 'b/c/d']
      var bWriter = b.createWriteStream()
      bWriter.write(bValues.map((v, i) => ({
        type: 'put',
        key: v,
        value: 4 + i
      })))
      bWriter.end((err) => {
        t.error(err, 'no error')
        var reader = b.createReadStream('b/')
        var expectedValues = [7, 6, 2, 3]
        reader.on('data', (data) => {
          t.equals(data.value, expectedValues.shift())
        })
        reader.on('end', () => {
          t.ok(expectedValues.length === 0)
          t.pass('stream ended ok')
          t.end()
        })
        reader.on('error', (err) => {
          console.log('Something went wrong', err)
          t.fail(err.message)
          t.end()
        })
      })
    }
  })
})

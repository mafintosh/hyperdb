var hyperdb = require('./')
var hypercore = require('hypercore')
var ram = require('random-access-memory')

var db = hyperdb([
  hypercore('./db', {valueEncoding: 'json'})
])

var tick = 0

// db.get('i42', console.log)

loop()

function loop () {
  if (tick === 10000) {
    return run()
  }
  console.log('adding', 'i' + tick)
  db.put('i' + (tick++), 'foo', loop)
}

// run()

function run () {
  db.put('hello', 'world', function () {
    console.log('next put')
    db.put('hi', 'world', function () {
      console.log('get')
      db.get('hello', console.log)
    })
  })
}

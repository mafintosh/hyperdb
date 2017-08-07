var crypto = require('crypto')
var hyperdb = require('./')
var hypercore = require('hypercore')

var db = hyperdb({map: a => a.value, reduce: (a, b) => a, feed: hypercore('test.db', {valueEncoding: 'json'})})
var i = 0
var time = Date.now()
var max = 100000

// get()
getAll()

function getAll () {
  var i = 0

  db.get('#' + i, loop)

  function loop (err, val) {
    if (err) throw err
    if (i === max) return console.log(Math.round(1000 * max / (Date.now() - time)))
    if (val !== i) throw new Error('invalid value: ' + i +' '+ val)

    if ((i % 10000) === 0) console.log('Got ' + i)
    i++
    db.get('#' + i, loop)
  }

}

// loop()

function get () {
  db.feeds[0].feed.ready(function () {
    var time = process.hrtime()

    console.log('getting')
    db.get('#0', function (err, val) {
      console.log('#0 ->', val, require('pretty-hrtime')(process.hrtime(time)))
      time = process.hrtime()
      db.get('#1024', function (err, val) {
        console.log('#1024 ->', val, require('pretty-hrtime')(process.hrtime(time)))
        time = process.hrtime()
        db.get('#10', function (err, val) {
          console.log('#10 ->', val, require('pretty-hrtime')(process.hrtime(time)))
          time = process.hrtime()
        })
      })
    })
  })
}

function loop () {
  if (i === max) {
    // console.log('hi')
    db.get('#0', console.log)

// for (var j = 0; j < db.feeds[0].length; j++) {
//   console.log(JSON.stringify(db.feeds[0][j]) + '\n')
//   // console.log(crypto.createHash('sha256').update(new Buffer(JSON.stringify(db.feeds[0][j]) + '\n')).digest('hex'))
// }

// db.feeds[0].createReadStream({valueEncoding: 'binary'}).on('data', function (data) {
//   console.log(data.toString())
//   // console.log(crypto.createHash('sha256').update(data).digest('hex'))
// })

    console.log(Math.round(1000 * max / (Date.now() - time)))
    return
  }

  if ((i % 10000) === 0) console.log('Inserted ' + i)
  // console.log('put #' + i)
  db.put('#' + i, i++, loop)
}


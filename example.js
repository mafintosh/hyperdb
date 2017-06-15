var hyperdb = require('./')
var ram = require('random-access-memory')

var db = hyperdb(ram, {
  map: a => a.value.toString(),
  reduce: (a, b) => a
})

for (var i = 0; i < 40; i++) {
  db.put('foo/i-' + i, 'i-' + i)
}

var map = {}

db.put('hi/hello', 'world', function () {
  db.put('hi/hej', 'verden', function () {
    db.put('hi/hi', 'verden', function () {
      db.get('hi/hello', function (err, val) {
        if (err) throw err

        var prefix = require('./lib/hash')(['hi'])
        var ite = db.iterator(prefix, function (node) {
          console.log('visiting', node.key)
          // if (map[node.key]) throw new Error('dup ' + node.key)
          // map[node.key] = true
        }, function () {
          console.log('(done)')
        })
        // ite(console.log)
      })
    })
  })
})

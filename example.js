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

db.put('hi/ho', 'ha', function () {
  db.put('hi/hello', 'world', function () {
    db.put('hi/hej', 'verden', function () {
      db.put('hi/hi', 'verden', function () {
        db.get('hi/hello', function (err, val) {
          if (err) throw err

          var ite = db.iterator('hi')

          ite.next(function loop (err, node) {
            if (err) throw err
            if (!node) return console.log('(end)')
            console.log(node.key)
            ite.next(loop)
          })
        })
      })
    })
  })
})

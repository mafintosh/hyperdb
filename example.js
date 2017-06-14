var hyperdb = require('./')
var ram = require('random-access-memory')

var db = hyperdb(ram)

for (var i = 0; i < 4000; i++) {
  db.put('i-' + i, 'i-' + i)
}

db.put('hello', 'world', function () {
  db.put('hej', 'verden', function () {
    db.put('hi', 'verden', function () {
      db.get('i-24', function (err, nodes) {
        if (err) throw err
        console.log(nodes[0])
      })
    })
  })
})

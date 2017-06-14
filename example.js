var hyperdb = require('./')
var ram = require('random-access-memory')

var db = hyperdb(ram)

db.put('hello', 'world', function () {
  db.put('hej', 'verden', function () {

  })
})

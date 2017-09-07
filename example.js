var hyperdb = require('./')

var db = hyperdb('./my.db', {valueEncoding: 'utf-8'})

db.put('/hello', 'world', function (err) {
  if (err) throw err
  db.get('/hello', function (err, nodes) {
    if (err) throw err
    console.log('/hello --> ' + nodes[0].value)
  })
})

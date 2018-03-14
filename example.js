var hyperdb = require('./')

var db = hyperdb('./my.db', {
  valueEncoding: 'utf-8',
  reduce: (a, b) => a
})

db.put('/hello', 'world', function (err) {
  if (err) throw err
  db.get('/hello', function (err, node) {
    if (err) throw err
    console.log('/hello --> ' + node.value)
  })
})

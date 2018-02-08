var hyperdb = require('./')

var db = hyperdb('./my.db', {valueEncoding: 'utf-8'})

db.put('/hello', 'world', function (err, node) {
  if (err) throw err
  console.log('inserted', node.key, node.value)
  db.get('/hello', function (err, nodes) {
    if (err) throw err
    console.log('/hello --> ' + nodes[0].value)
  })
})

var random = require('./helpers/random')
var bench = require('./helpers/bench')

bench('bulk write 1e5 documents, ~10 per common prefix', function (b, db) {
  var data = random.fullData({
    numKeys: 1e5,
    maxKey: 1e4
  })
  b.start()
  db.batch(data, function (err) {
    if (err) throw err
    b.end()
    b.done()
  })
})

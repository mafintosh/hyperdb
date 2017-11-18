var hyperdb = require('./')

var db = hyperdb()
var n = 100
var snapshot = db

db.put('hello', 'world')

for (var i = 0; i < n; i++) {
  // if (i === 100) snapshot = db.snapshot()
  db.put('i/' + (i % 10) + '/#' + i, '#' + i)
}

db.put('i/0/#0', 'new value')

db.get('hello', console.log)
return

// db.get('4', {prefix: true}, console.log)
// db.put('hello', 'world')
// db.put('hej', 'verden')
// db.put('hejsa', 'verden')

snapshot.createReadStream('/', {recursive: false})
  .on('data', nodes => console.log(nodes[0].path.join(''), nodes[0].key, '-->', nodes[0].value))
  .on('end', _ => console.log('(done)'))

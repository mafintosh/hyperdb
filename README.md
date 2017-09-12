# hyperdb

Distributed scalable database.

```
npm install hyperdb
```

## Note

Latest release is the 1.0.0-rc1

The storage format might change before a stable 1.0.0 release
but the api semantics most likely wont

## Usage

``` js
var hyperdb = require('hyperdb')

var db = hyperdb('./my.db', {valueEncoding: 'utf-8'})

db.put('/hello', 'world', function (err) {
  if (err) throw err
  db.get('/hello', function (err, nodes) {
    if (err) throw err
    console.log('/hello --> ' + nodes[0].value)
  })
})
```

## API

#### `var db = hyperdb(storage, [key], [options])`

Create a new hyperdb.

`storage` is a function that is called with every filename hyperdb needs to
operate on. There are many providers for the
[abstract-random-access](https://github.com/juliangruber/abstract-random-access)
interface. e.g.
```js
   var ram = require('random-access-memory')
   var feed = hyperdb(function (filename) {
     // filename will be one of: data, bitfield, tree, signatures, key, secret_key
     // the data file will contain all your data concattenated.

     // just store all files in ram by returning a random-access-memory instance
     return ram()
   })
```

`key` is a `Buffer` containing the local feed's public key. If you do not set
this the public key will be loaded from storage. If no key exists a new key pair
will be generated.

#### `db.on('ready')`

Emitted exactly once: when the db is fully ready and all static properties have
been set. You do not need to wait for this when calling any async functions.

#### `db.on('error', err)`

Emitted if there was a critical error before `db` is ready.

#### `db.put(key, value, [callback])`

Insert a new value. Will merge any previous values seen for this key.

#### `db.batch(batch, [callback])`

Insert a batch of values efficiently. A batch should be an array of objects that look like this:

``` js
{
  type: 'put',
  key: someKey,
  value: someValue
}
```

#### `db.get(key, callback)`

Lookup a string `key`. Returns a nodes array with the current values for this key.
If there is no current conflicts for this key the array will only contain a single node.

#### `db.local`

Your local writable feed. You have to get an owner of the hyperdb to authorize you to have your
writes replicate. The first person to create the hyperdb is the first owner.

#### `db.authorize(key, [callback])`

Authorize another peer to write to the hyperdb.

To get another peer to authorize you you'd usually do something like

``` js
myDb.on('ready', function () {
  console.log('You local key is ' + myDb.local.key.toString('hex'))
  console.log('Tell an owner to authorize it')
})
```

#### `unwatch = db.watch(folderOrKey, onchange)`

Watch a folder and get notified anytime a key inside this folder
has changed.

``` js
db.watch('/foo/bar', function () {
  console.log('folder has changed')
})

...

db.put('/foo/bar/baz', 'hi') // triggers the above
```

#### `var stream = db.replicate([options])`

Create a replication stream. Options include:

``` js
{
  live: false // set to true to keep replicating
}
```

## License

MIT

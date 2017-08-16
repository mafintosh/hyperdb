# hyperdb

Distributed scalable database.

```
npm install hyperdb
```

## Note

Latest release is the 1.0.0-rc1

The storage format might change before a stable 1.0.0 release
but the api semantics most likely wont

## API

#### `var db = hyperdb(storage, [key], [options])`

Create a new hyperdb.

Options include:

``` js
{
  valueEncoding: 'json' | 'utf-8' | 'binary' | someEncoder, // defaults to binary
  sparse: false
}
```

#### `db.on('ready')`

Emitted when the db is fully ready and all static properties have been set.
You do not need to wait for this when calling any async functions.

#### `db.on('error', err)`

Emitted if there was a critical error before the is ready.

#### `db.put(key, value, [callback])`

Insert a new value. Will merge any previous values seen for this key.

#### `db.get(key, callback)`

Lookup a key. Returns a nodes array with the current values for this key.
Is there is no current conflicts for this key the array will only contain a single node.

#### `db.local`

Your local writable feed. You have to get an owner of the hyperdb to authorize you to have your
writes replicate. The first person to create the hyperdb is the first owner.

#### `db.authorize(key, [callback])`

Authorize another peer to write to the hyperdb.

To get another peer to authorize you you'd usually do something similar to this

``` js
myDb.on('ready', function () {
  console.log('You local key is ' + myDb.local.key.toString('hex'))
  console.log('Tell an owner to authorize it')
})
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

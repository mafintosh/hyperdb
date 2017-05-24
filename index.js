var sodium = require('sodium-universal')
var alru = require('array-lru')
var allocUnsafe = require('buffer-alloc-unsafe')
var toBuffer = require('to-buffer')
var thunky = require('thunky')

var KEY = allocUnsafe(sodium.crypto_shorthash_KEYBYTES).fill(0)

module.exports = DB

function DB (feeds) {
  if (!(this instanceof DB)) return new DB(feeds)
  if (feeds.length < 1) throw new Error('Must pass at least one feed')

  var self = this

  this.cache = alru(65536)
  this.feeds = feeds
  this.writer = null
  this.ready = thunky(open)

  this.readable = true
  this.writable = false

  function open (cb) {
    self._open(cb)
  }
}

DB.prototype._open = function (cb) {
  var missing = this.feeds.length
  var error = null
  var self = this

  for (var i = 0; i < this.feeds.length; i++) {
    this.feeds[i].ready(onready)
  }

  function onready (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)

    for (var i = 0; i < self.feeds.length; i++) {
      if (self.feeds[i].writable) {
        self.writer = self.feeds[i]
        self.writable = true
      }
    }

    cb(null)
  }
}

DB.prototype.close = function (cb) {
  var self = this
  self.ready(function (err) {
    if (err) return cb(err)

    self._close(cb)
  })
}

DB.prototype._close = function (cb) {
  var missing = this.feeds.length
  var error = null

  for (var i = 0; i < this.feeds.length; i++) {
    this.feeds[i].close(onclose)
  }

  var self = this

  function onclose (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)

    self.readable = false
    self.writable = false

    cb(null)
  }
}

DB.prototype._list = function (head, path, cb) {
  var self = this

  if (!head) return cb(null, [])

  var cmp = compare(head.path, path)
  var ptrs = head.pointers[cmp]

  if (cmp === path.length) { // root
    self._getAll(ptrs, cb)
    return
  }

  self._closer(path, cmp, ptrs, cb)
}

DB.prototype._closer = function (path, cmp, ptrs, cb) {
  var target = path[cmp]
  var self = this

  this._getAll(ptrs, function (err, nodes) {
    if (err) return cb(err)

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i]

      if (node.path[cmp] === target) {
        self._list(node, path, cb)
        return
      }
    }

    cb(null, [])
  })
}

DB.prototype.get = function (key, cb) {
  var h = hash(toBuffer(key))
  var path = splitHash(h)

  var self = this

  this.head(function (err, head) {
    if (err) return cb(err)
    self._get(head, key, path, cb)
  })
}

DB.prototype._get = function (head, key, path, cb) {
  if (head.key === key) return cb(null, head)

  var cmp = compare(head.path, path)
  var ptrs = head.pointers[cmp]

  if (cmp === head.path.length - 1) {
    console.log('special (hash collisions values)')
    return
  }

  if (!ptrs.length) return cb(new Error('not found'))

  var target = path[cmp]
  var self = this

  this._getAll(ptrs, function (err, nodes) {
    if (err) return cb(err)

    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].path[cmp] === target) return self._get(nodes[i], key, path, cb)
    }

    cb(new Error('not found'))
  })
}

DB.prototype.put = function (key, value, cb) {
  var h = hash(toBuffer(key))
  var path = splitHash(h)

  var self = this
  var end = 0
  var pointers = []

  this.head(function (err, head) {
    if (err) return cb(err)

    loop(null, null)

    function done () {
      var node = {
        seq: self.writer.length,
        feed: self.writer.key.toString('hex'),
        key: key,
        path: path,
        value: value,
        pointers: pointers
      }

      self.writer.append(node, cb)
    }

    function loop (err, entries) {
      if (err) return cb(err)
      if (end === path.length) return done()

      if (entries) {
        var offset = end++

        entries = entries
          .filter(function (entry) {
            return entry.path[offset] !== path[offset]
          })
          .map(function (entry) {
            return {feed: entry.feed, seq: entry.seq}
          })

        entries.push({feed: self.writer.key.toString('hex'), seq: self.writer.length})
        pointers.push(entries)
      }

      self._list(head, path.slice(0, end), loop)
    }
  })
}

DB.prototype._getAll = function (pointers, cb) {
  if (!pointers) return cb(null, [])

  var all = new Array(pointers.length)
  var missing = all.length
  var error = null
  var self = this

  pointers.forEach(function (ptr, i) {
    self._getValue(ptr.seq, function (err, node) {
      if (err) error = err
      if (node) all[i] = node
      if (--missing) return

      if (error) cb(error)
      else cb(null, all)
    })
  })
}

DB.prototype._getValue = function (seq, cb) {
  var self = this
  var cached = this.cache.get(seq)
  if (cached) return process.nextTick(cb, null, cached)

  self.writer.get(seq, function (err, node) {
    if (err) return cb(err)
    self.cache.set(seq, node)
    cb(null, node)
  })
}

DB.prototype.head = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    if (!self.writer.length) return cb(null, null)
    self._getValue(self.writer.length - 1, cb)
  })
}

function hash (key) {
  var out = allocUnsafe(8)
  sodium.crypto_shorthash(out, key, KEY)
  return out
}

function splitHash (hash) {
  var list = []
  for (var i = 0; i < hash.length; i++) {
    factor(hash[i], 4, 4, list)
  }

  return list
}

function compare (a, b) {
  var idx = 0
  while (idx < a.length && a[idx] === b[idx]) idx++
  return idx
}

function factor (n, b, cnt, list) {
  while (cnt--) {
    var r = n & (b - 1)
    list.push(r)
    n -= r
  }
}

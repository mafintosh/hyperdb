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
  this.ready = thunky(open)

  this.readable = true
  this.writable = false

  this._feeds = []
  this._feedsByKey = {}
  this._writer = null

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
      var wrap = new Feed(self.feeds[i])

      self._feeds[i] = wrap
      self._feedsByKey[wrap.feed.key.toString('hex')] = wrap

      if (self.feeds[i].writable) {
        self._writer = wrap
        self.writable = true
      }
    }

    cb(null)
  }
}

DB.prototype._listAll = function (head, ends, path, cb) {
  if (!head) return cb(null, [])

  var self = this

  this._list(head, path, null, function (err, entries) {
    if (err) return cb(err)

    var other = self._feedsByKey[head.feed] === self._feeds[0] ? self._feeds[1] : self._feeds[0]

    other.head(function (err, otherHead) {
      if (err) return cb(err)
      self._list(otherHead, path, head.heads, function (err, otherEntries) {
        if (err) return cb(err)

        if (otherEntries.length) {
          console.log('head', head)
          console.log('other entries', otherEntries)
          console.log('entries', entries)
          process.exit()
        }

        cb(null, entries)
      })
    })
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

DB.prototype._list = function (head, path, min, cb) {
  var self = this

  if (!head) return cb(null, [])

  var cmp = compare(head.path, path)
  var ptrs = head.pointers[cmp]

  if (min) {
    ptrs = ptrs.filter(function (p) {
      var m = min[p.feed] || 0
      return p.seq >= m
    })
  }

  if (cmp === path.length) {
    self._getAll(ptrs, cb)
    return
  }

  self._closer(path, cmp, ptrs, min, cb)
}

DB.prototype._closer = function (path, cmp, ptrs, min, cb) {
  var target = path[cmp]
  var self = this

  this._getAll(ptrs, function (err, nodes) {
    if (err) return cb(err)

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i]

      if (node.path[cmp] === target) {
        self._list(node, path, min, cb)
        return
      }
    }

    cb(null, [])
  })
}

DB.prototype.put = function (key, val, cb) {
  var h = hash(toBuffer(key))
  var path = splitHash(h)
  var self = this
  var end = 0
  var pointers = []

  this.head(function (err, head) {
    if (err) return cb(err)

    var ends = {}
    for (var i = 0; i < self.feeds.length; i++) {
      if (self.feeds[i] === self._writer.feed) continue
      ends[self.feeds[i].key.toString('hex')] = self.feeds[i].length
    }

    loop(null, null)

    function done () {
      var node = {
        seq: self._writer.feed.length,
        feed: self._writer.feed.key.toString('hex'),
        heads: ends,
        key: key,
        path: path,
        value: val,
        pointers: pointers
      }

      self._writer.append(node, cb)
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

        entries.push({feed: self._writer.feed.key.toString('hex'), seq: self._writer.feed.length})
        pointers.push(entries)
      }

      self._listAll(head, ends, path.slice(0, end), loop)
    }
  })
}

DB.prototype.head = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    var heads = []
    var i = 0

    loop()

    function loop () {
      if (i === self._feeds.length) {
        if (!heads.length) return cb(null, null)
        if (heads.length === 1) return cb(null, heads[0])

        if (heads.length > 2) throw new Error('only two supported now')

        var a = heads[0]
        var b = heads[1]

        if (a.heads[b.feed] > b.seq) return cb(null, a)
        cb(null, b)
        return
      }

      self._feeds[i++].head(function (err, head) {
        if (err) return cb(err)
        if (head) heads.push(head)
        loop()
      })
    }
  })
}

DB.prototype._getAll = function (pointers, cb) {
  if (!pointers || !pointers.length) return cb(null, [])

  var all = new Array(pointers.length)
  var missing = all.length
  var error = null
  var self = this

  pointers.forEach(function (ptr, i) {
    var feed = self._feedsByKey[ptr.feed]

    feed.get(ptr.seq, function (err, node) {
      if (err) error = err
      if (node) all[i] = node
      if (--missing) return

      if (error) cb(error)
      else cb(null, all)
    })
  })
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

function Feed (feed) {
  this.feed = feed
  this.cache = alru(65536)
}

Feed.prototype.head = function (cb) {
  var self = this

  this.feed.ready(function (err) {
    if (err) return cb(err)
    if (!self.feed.length) return cb(null, null)
    self.get(self.feed.length - 1, cb)
  })
}

Feed.prototype.append = function (val, cb) {
  this.feed.append(val, cb)
}

Feed.prototype.get = function (i, cb) {
  var self = this

  var cached = this.cache.get(i)
  if (cached) return process.nextTick(cb, null, cached)

  this.feed.get(i, function (err, val) {
    if (err) return cb(err)
    self.cache.set(i, val)
    cb(null, val)
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

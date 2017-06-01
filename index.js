var sodium = require('sodium-universal')
var alru = require('array-lru')
var allocUnsafe = require('buffer-alloc-unsafe')
var toBuffer = require('to-buffer')
var thunky = require('thunky')

var KEY = allocUnsafe(sodium.crypto_shorthash_KEYBYTES).fill(0)

module.exports = DB

function DB (feeds) {
  if (!(this instanceof DB)) return new DB(feeds)

  var self = this

  this.feeds = feeds
  this.ready = thunky(open)
  this._writer = null

  function open (cb) {
    self._open(cb)
  }
}

DB.prototype.replicate = function (opts) {
  if (!opts) opts = {}

  var self = this

  opts.expectedFeeds = this.feeds.length
  opts.stream = this.feeds[0].replicate(opts)

  self.feeds[0].ready(function () {
    for (var i = 1; i < self.feeds.length; i++) {
      self.feeds[i].replicate(opts)
    }
  })

  return opts.stream
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
        self._writer = self.feeds[i]
      }
    }

    cb(null)
  }
}

DB.prototype.get = function (key, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    getHeads(self.feeds, function (err, heads) {
      if (err) return cb(err)

      var i = 0
      var nodes = []

      loop(null)

      function loop (err) {
        if (err) return cb(err)
        if (i >= heads.length) return done()
        var head = heads[i++]

        self._get(head, key, nodes, loop)
      }

      function done () {
        nodes = dedup(nodes, heads)
        cb(null, nodes)
      }
    })
  })
}

DB.prototype._get = function (head, key, result, cb) {
  if (!head) return cb(null)

  if (head.key === key) {
    result.push(head)
    return cb(null)
  }

  var path = toPath(key)
  var cmp = compare(head.path, path)
  var ptrs = head.pointers[cmp]

  if (!ptrs.length) return cb(null)
  var target = path[cmp]
  var self = this

  this._getAll(ptrs, function (err, nodes) {
    if (err) return cb(err)

    var i = 0
    loop(null)

    function loop (err) {
      if (err) return cb(err)
      if (i === nodes.length) return cb(null)

      var node = nodes[i++]

      if (node.path[cmp] === target) {
        return self._get(node, key, result, loop)
      }

      process.nextTick(loop)
    }
  })
}

DB.prototype.put = function (key, val, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    getHeads(self.feeds, function (err, heads) {
      if (err) return cb(err)
      if (heads.every(isNull)) return self._init(key, val, cb)

      var path = toPath(key)
      var result = []
      var i = 0
      var j = 0
      var pointers = []

      heads = heads.filter(x => x)
      loop()

      function filter (result, val, i) {
        var me = self._writer.key.toString('hex')

        result = result.filter(function (r) {
          if (r.key === key) return false
          if (r.feed === me && r.path[i] === val) {
            return false
          }
          return true
        })

        result = result.map(function (r) {
          return {feed: r.feed, seq: r.seq}
        })

        result.push({
          feed: me,
          seq: self._writer.length
        })

        return result
      }

      function done () {
        var node = {
          feed: self._writer.key.toString('hex'),
          seq: self._writer.length,
          key: key,
          pointers: pointers,
          path: path,
          value: val,
          heads: self.feeds
            .map(function (f) {
              return f !== self._writer && {
                feed: f.key.toString('hex'),
                length: f.length
              }
            })
            .filter(function (f) {
              return f
            })
        }

        self._writer.append(node, cb)
      }

      function loop (err, nodes) {
        if (err) return cb(err)

        if (nodes) {
          pointers.push(filter(nodes, path[i], i))
          i++
        }

        if (i === path.length) return done()
        self._listHeads(heads, path.slice(0, i), loop)
      }
    })
  })
}

DB.prototype.list = function (path, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    getHeads(self.feeds, function (err, heads) {
      if (err) return cb(err)

      self._listHeads(heads, path, cb)
    })
  })
}

DB.prototype._listHeads = function (heads, path, cb) {
  var self = this
  var i = 0
  var result = []

  loop(null, null)

  function loop (err, nodes) {
    if (err) return cb(err)

    if (nodes) {
      for (var j = 0; j < nodes.length; j++){
        result.push(nodes[j])
      }
    }

    if (i === heads.length) {
      return cb(null, dedupKeys(result, heads))
    }

    self._list(heads[i++], path, loop)
  }
}

DB.prototype._list = function (head, path, cb) {
  var self = this

  if (!head) return cb(null, [])

  var cmp = compare(head.path, path)
  var ptrs = head.pointers[cmp]

  if (cmp === path.length) {
    self._getAll(ptrs, cb)
    return
  }

  self._closer(path, cmp, ptrs, cb)
}

DB.prototype._init = function (key, val, cb) {
  var self = this
  var node = {
    feed: this._writer.key.toString('hex'),
    seq: 0,
    key: key,
    pointers: toPath(key).map(function (v) {
      return [{feed: self._writer.key.toString('hex'), seq: 0}]
    }),
    path: toPath(key),
    value: val,
    heads: this.feeds
      .map(function (f) {
        return f !== self._writer && {
          feed: f.key.toString('hex'),
          length: f.length
        }
      })
      .filter(function (f) {
        return f
      })
  }

  this._writer.append(node, cb)
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

DB.prototype._getAll = function (pointers, cb) {
  if (!pointers || !pointers.length) return cb(null, [])

  var all = new Array(pointers.length)
  var missing = all.length
  var error = null
  var self = this

  pointers.forEach(function (ptr, i) {
    var feed = find(ptr.feed)

    feed.get(ptr.seq, function (err, node) {
      if (err) error = err
      if (node) all[i] = node
      if (--missing) return

      if (error) cb(error)
      else cb(null, all)
    })
  })

  function find (key) {
    for (var i = 0; i < self.feeds.length; i++) {
      if (self.feeds[i].key.toString('hex') === key) return self.feeds[i]
    }
    return null
  }
}


function getHeads (feeds, cb) {
  var error = null
  var heads = []
  var missing = feeds.length

  feeds.forEach(function (feed, i) {
    head(feed, function (err, h) {
      if (err) error = err
      else heads[i] = h

      if (--missing) return
      cb(error, heads)
    })
  })
}

function dedupKeys (nodes, heads) {
  nodes.sort(function (a, b) {
    return a.key.localeCompare(b.key)
  })

  var batch = nodes.slice(0, 1)
  var all = []

  for (var i = 1; i < nodes.length; i++) {
    if (nodes[i - 1].key === nodes[i].key) {
      batch.push(nodes[i])
    } else {
      all = all.concat(dedup(batch, heads))
      batch = [nodes[i]]
    }
  }

  return all.concat(dedup(batch, heads))
}

function dedup (nodes, heads) {
  nodes = nodes.filter(function (n, i) {
    return indexOf(n) === i
  })

  nodes = nodes.filter(function (n) {
    return !nodes.some(function (o) {
      if (o.feed === n.feed && o.seq > n.seq) return true
      return o.heads.some(function (head) {
        return head.feed === n.feed && head.length > n.seq
      })
    })
  })

  return nodes

  function indexOf (n) {
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].feed === n.feed && nodes[i].seq === n.seq) return i
    }
    return -1
  }
}

function head (feed, cb) {
  if (!feed.length) return cb(null, null)
  feed.get(feed.length - 1, cb)
}

function toPath (key)  {
  var arr = splitHash(hash(toBuffer(key)))
  arr.push(key)
  return arr

  // var ps = key.split('')
  // while (ps.length < 10) ps.push('0')
  // ps.push(key)
  // return ps
}

function isNull (v) {
  return v === null
}

function compare (a, b) {
  var idx = 0
  while (idx < a.length && a[idx] === b[idx]) idx++
  return idx
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

function factor (n, b, cnt, list) {
  while (cnt--) {
    var r = n & (b - 1)
    list.push(r)
    n -= r
  }
}

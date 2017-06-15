var hypercore = require('hypercore')
var thunky = require('thunky')
var path = require('path')
var raf = require('random-access-file')
var protocol = require('hypercore-protocol')
var inherits = require('inherits')
var events = require('events')
var toBuffer = require('to-buffer')
var mutexify = require('mutexify')
var peer = require('./lib/peer')
var hash = require('./lib/hash')

module.exports = HyperDB

function HyperDB (storage, key, opts) {
  if (!(this instanceof HyperDB)) return new HyperDB(storage, key, opts)

  if (isOptions(key)) {
    opts = key
    key = null
  }
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  var self = this

  this._lock = mutexify()
  this._map = opts.map
  this._reduce = opts.reduce

  this.setMaxListeners(0) // can be removed once our dynamic 'add-feed' thing is removed
  this.storage = typeof storage === 'string' ? fileStorage : storage
  this.key = key ? toBuffer(key, 'hex') : null
  this.discoveryKey = null
  this.sparse = opts.sparse
  this.local = null
  this.peers = []
  this.ready = thunky(open)
  this.ready()

  function fileStorage (name) {
    return raf(name, {directory: storage})
  }

  function open (cb) {
    self._open(cb)
  }
}

inherits(HyperDB, events.EventEmitter)

HyperDB.prototype._createFeed = function (dir, key) {
  var self = this
  var feed = hypercore(storage, key, {sparse: this.sparse})

  feed.on('append', onappend)

  return feed

  function onappend () {
    self.emit('append', feed)
  }

  function storage (name) {
    return self.storage(path.join(dir, name))
  }
}

HyperDB.prototype._open = function (cb) {
  var self = this
  var source = peer(this._createFeed('source', this.key))

  source.feed.ready(function (err) {
    if (err) return cb(err)

    self.key = source.feed.key
    self.discoveryKey = source.feed.discoveryKey
    self.peers.push(source)
    self.emit('add-feed', source.feed)

    if (source.feed.writable) self.local = source
    if (self.local) return onme()

    self.local = peer(self._createFeed('local'))
    self.local.feed.ready(onme)
  })

  function done () {
    var missing = self.peers.length
    var error = null

    for (var i = 0; i < self.peers.length; i++) self.peers[i].feed.ready(onready)

    function onready (err) {
      if (err) error = err
      if (--missing) return
      cb(null)
    }
  }

  function onme (err) {
    if (err) return cb(err)
    if (self.local.length > 0) return update(null)
    self.local.header({type: 'hyperdb', version: 0}, update)
  }

  function update (err) {
    if (err) return cb(err)
    self._update({cached: true}, done)
  }
}

HyperDB.prototype.put = function (key, value, cb) {
  if (typeof value === 'string') value = toBuffer(value)
  if (!cb) cb = noop

  var self = this
  var parts = split(key)

  this.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (release) {
      self._heads(function (err, heads, set) {
        if (err) return done(err)
        if (!set) return self._init(parts, value, done)
        self._put(heads, parts, value, done)
      })

      function done (err) {
        release(cb, err)
      }
    })
  })
}

HyperDB.prototype._put = function (heads, key, value, cb) {
  var keyHash = hash(key)
  var self = this
  var i = 0
  var me = 0
  var seq = self.local.feed.length
  var pointers = []
  // console.log('_put', key, hash(key))
  // console.log(heads)

  loop(null, null)

  function done () {
    var node = {
      heads: [],
      pointers: pointers,
      key: key.join('/'),
      value: value
    }

    self.local.append(node, cb)
  }

  function filter (nodes, target, i) {
    var result = []

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      if (node.key === key) continue
      if (node.feed === me && getHash(node)[i] === target) continue

      result.push({
        feed: node.feed,
        seq: node.seq,
        target: getHash(node)[i]
      })
    }

    result.push({
      feed: me,
      seq: seq,
      target: target
    })

    return result
  }

  function loop (err, nodes) {
    if (i === keyHash.length) return done()

    if (nodes) {
      pointers.push(filter(nodes, keyHash[i], i))
      i++
    }

    list(heads, keyHash.slice(0, i), loop)
  }

  function list (heads, prefix, cb) {
    self._list(heads[0], prefix, cb)
  }
}

HyperDB.prototype._init = function (key, value, cb) {
  var pointers = []
  var h = hash(key)
  for (var i = 0; i < h.length; i++) {
    pointers.push([{feed: 0, seq: 1, target: h[i]}])
  }

  var node = {
    heads: [],
    pointers: pointers,
    key: key.join('/'),
    value: value
  }

  this.local.append(node, cb)
}

HyperDB.prototype.get = function (key, cb) {
  var parts = split(key)
  var self = this

  this._heads(function (err, heads, set) {
    if (err) return cb(err)
    if (!set) return cb(null, null)
    self._get(heads, parts, cb)
  })
}

HyperDB.prototype._get = function (heads, key, cb) {
  var keyHash = hash(key)
  var self = this
  var result = []

  get(heads[0], key, keyHash, result, function (err) {
    if (err) return cb(err)

    // TODO: do this iteratively instead
    if (self._map) result = result.map(self._map)
    if (self._reduce) result = result.length ? result.reduce(self._reduce) : null

    cb(null, result)
  })

  function get (head, key, hash, result, cb) {
    if (head.key === key.join('/')) {
      result.push(head)
      return cb(null)
    }

    var cmp = compare(hash, getHash(head))
    var ptrs = head.pointers[cmp]
    var relevant = []
    var target = keyHash[cmp]

    for (var i = 0; i < ptrs.length; i++) {
      var p = ptrs[i]
      // 3 --> our MAX hash value (2 bits)
      if (p.target === target || p.target > 3) relevant.push(p)
    }

    self._getAll(relevant, function (err, nodes) {
      if (err) return cb(err)
      var i = 0
      loop(null)

      function loop (err) {
        if (err) return cb(err)
        if (i === nodes.length) return cb(null)

        var node = nodes[i++]

        if (getHash(node)[cmp] === target) get(node, key, hash, result, loop)
        else process.nextTick(loop)
      }

    })
  }
}

HyperDB.prototype.iterator = function (prefix, onnode, cb) {
  var self = this
  var stack = []

  self._heads(function (err, heads) {
    if (err) return cb(err)

    visit(heads[0], prefix, cb)

    function visit (head, prefix, cb) {
      self._list(head, prefix, function (err, list) {
        if (err) return cb(err)

        var i = 0
        loop(null)

        function loop (err) {
          if (err) return cb(err)

          if (i === list.length) return cb(null)
          var node = list[i++]

          if (prefix.length === getHash(node).length - 1) {
            onnode(node)
            return process.nextTick(cb, null)
          }

          visit(head, getHash(node).slice(0, prefix.length + 1), loop)
        }
      })
    }
  })


  // heads()

  // return function (cb) {
  //   heads(function (err, heads) {
  //     if (err) return cb(err)
  //   })
  // }
}

HyperDB.prototype._closer = function (prefix, cmp, ptrs, cb) {
  var target = prefix[cmp]
  var self = this

  this._getAll(ptrs, function (err, nodes) {
    if (err) return cb(err)

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i]
      var nodeHash = getHash(node)

      if (nodeHash[cmp] === target) {
        self._list(node, prefix, cb)
        return
      }
    }

    cb(null, [])
  })
}

HyperDB.prototype._list = function (head, prefix, cb) {
  if (!head) return cb(null)

  var headHash = getHash(head)

  var cmp = compare(prefix, headHash)
  var ptrs = head.pointers[cmp]

  if (cmp === prefix.length) {
    this._getAll(ptrs, cb)
    return
  }

  this._closer(prefix, cmp, ptrs, cb)
}

HyperDB.prototype._getAll = function (pointers, cb) {
  var missing = pointers.length
  var self = this
  var error = null
  var result = new Array(pointers.length)

  if (!missing) return cb(null, result)

  pointers.forEach(function (p, i) {
    self.peers[p.feed].get(p.seq, function (err, node) {
      if (err) error = err
      if (node) result[i] = node
      if (--missing) return
      cb(error, result)
    })
  })
}

HyperDB.prototype._heads = function (cb) {
  var heads = []
  var missing = this.peers.length
  var error = null
  var set = 0

  this.peers.forEach(function (peer, i) {
    peer.head(function (err, head) {
      if (err) error = err

      if (!err) {
        if (head) set++
        heads[i] = head
      }

      if (--missing) return
      cb(error, heads, set)
    })
  })
}

HyperDB.prototype._update = function (opts, cb) {
  cb(null)
}

function noop () {}

function getHash (node) {
  return hash(split(node.key))
}

function isOptions (opts) {
  return !!(opts && typeof opts !== 'string' && !Buffer.isBuffer(opts))
}

function compare (a, b) {
  var idx = 0
  while (idx < a.length && a[idx] === b[idx]) idx++
  return idx
}

function split (key) {
  var list = key.split('/')
  if (list[0] === '') list.shift()
  if (list[list.length - 1] === '') list.pop()
  return list
}

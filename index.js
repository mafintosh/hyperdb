var hash = require('./lib/hash')
var writer = require('./lib/writer')
var hypercore = require('hypercore')
var remove = require('unordered-array-remove')
var raf = require('random-access-file')
var mutexify = require('mutexify')
var thunky = require('thunky')
var codecs = require('codecs')
var events = require('events')
var inherits = require('inherits')
var toBuffer = require('to-buffer')
var Readable = require('stream').Readable
var once = require('once')
var protocol = null // lazy load on replicate

module.exports = DB

function DB (storage, key, opts) {
  if (!(this instanceof DB)) return new DB(storage, key, opts)
  events.EventEmitter.call(this)

  if (isOptions(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  var self = this

  this.key = key ? toBuffer(key, 'hex') : null
  this.discoveryKey = null
  this.local = null
  this.source = null
  this.opened = false
  this.sparse = !!opts.sparse

  this._codec = opts.valueEncoding ? codecs(opts.valueEncoding) : null
  this._storage = typeof storage === 'string' ? fileStorage : storage
  this._map = opts.map || null
  this._reduce = opts.reduce || null
  this._writers = []
  this._lock = mutexify() // unneeded once we support batching
  this._localWriter = null
  this._updates = 0
  this.setMaxListeners(0) // use ._streams array instead

  this.ready = thunky(open)
  this.ready(onready)

  function onready (err) {
    self.opened = !err
    if (err) self.emit('error', err)
    else self.emit('ready')
  }

  function open (cb) {
    self._open(cb)
  }

  function fileStorage (name) {
    return raf(name, {directory: storage})
  }
}

inherits(DB, events.EventEmitter)

DB.prototype.batch = function (batch, cb) {
  if (!cb) cb = noop
  if (!batch.length) return process.nextTick(cb, null)

  var self = this
  var nodes = []
  var i = 0

  this._lock(function (release) {
    loop(null)

    function loop (err) {
      if (err) return done(err)

      if (i === batch.length) {
        self._localWriter.batch(nodes, done)
        return
      }

      var b = batch[i++]
      self._put(b.key, b.value, nodes, loop)
    }

    function done (err) {
      release(cb, err)
    }
  })
}

DB.prototype.heads = function (cb) {
  var result = []
  var error = null
  var self = this
  var missing = 0
  var i = 0

  this.ready(onready)

  function onready (err) {
    if (err) return cb(err)

    for (; i < self._writers.length; i++) {
      missing++
      self._writers[i].head(onhead)
    }
  }

  function onhead (err, val, updated) {
    if (updated) onready(null)

    if (err) error = err
    else if (val) result[val.feed] = val
    if (--missing) return

    if (error) return cb(error)

    for (var i = 0; i < result.length; i++) {
      var head = result[i]
      if (!head) continue

      for (var j = 0; j < head.clock.length; j++) {
        if (result[j] && result[j].seq < head.clock[j]) result[j] = null
      }
    }

    cb(null, result.filter(Boolean))
  }
}

DB.prototype.authorize = function (key, cb) {
  this._createWriter(toBuffer(key, 'hex'))
  this.put('', '', cb)
}

DB.prototype.replicate = function (opts) {
  if (!protocol) protocol = require('hypercore-protocol')
  if (!opts) opts = {}

  var len = this._writers.length
  opts.expectedFeeds = len

  var self = this
  var stream = protocol(opts)

  opts.stream = stream

  this.ready(function (err) {
    if (err) return stream.destroy(err)
    if (stream.destroyed) return

    for (var i = 0; i < self._writers.length; i++) {
      self._writers[i].feed.replicate(opts)
    }

    self.on('_writer', onwriter)
    if (!self.sparse) {
      onappend()
      self.on('append', onappend)
    }

    stream.on('close', onclose)
    stream.on('end', onclose)

    function onclose () {
      self.removeListener('append', onappend)
      self.removeListener('_writer', onwriter)
    }

    function onappend () {
      // hack! make api in hypercore-protocol for this
      if (stream.destroyed) return
      stream.expectedFeeds += 1e9
      self._update(function () {
        stream.expectedFeeds -= 1e9
        stream.expectedFeeds += (self._writers.length - len)
        len = self._writers.length
        if (!stream.expectedFeeds) stream.finalize()
      })
    }

    function onwriter (w) {
      if (stream.destroyed) return
      w.feed.replicate(opts)
    }
  })

  return stream
}

DB.prototype.watch = function (key, onchange) {
  var self = this
  var prev = null

  update()
  this.on('append', update)

  return function unwatch () {
    onchange = noop
    self.removeListener('append', update)
  }

  function update () {
    self._closest(key, check)
  }

  function check (err, nodes) {
    if (err) return onchange(err)

    if (!prev) {
      prev = nodes
      return
    }

    var changed = nodes.length !== prev.length
    for (var i = 0; i < nodes.length && !changed; i++) {
      changed = nodes[i].feed !== prev[i].feed || nodes[i].seq !== prev[i].seq
    }

    prev = nodes
    if (changed) onchange(null)
  }
}

DB.prototype._closest = function (key, cb) {
  var nodes = []
  var len = hash(key).length

  this._get(key, false, null, onvisit, done)

  function onvisit (node, matchLength, head) {
    if (!head) return
    if (matchLength >= len && noDup(nodes, node)) {
      nodes.push(node)
    }
  }

  function done (err) {
    if (err) return cb(err)
    cb(null, nodes.sort(sortNodes))
  }
}

DB.prototype.put = function (key, value, cb) {
  var self = this

  this._lock(function (release) {
    self._put(key, value, null, function (err) {
      release(cb, err)
    })
  })
}

DB.prototype._put = function (key, value, batch, cb) {
  if (!cb) cb = noop

  var self = this
  var path = hash(key, true)

  this.heads(function (err, heads) {
    if (err) return cb(err)

    if (!self._localWriter) self._localWriter = self._createWriter(self.local.key, 'local')

    var feed = self._localWriter.id
    var clock = []
    var i = 0
    var len = self._localWriter.feed.length

    if (batch && batch.length) {
      len += batch.length
      heads[feed] = batch[batch.length - 1]
    }

    for (i = 0; i < self._writers.length; i++) {
      clock.push(i === feed ? 0 : self._writers[i].feed.length)
    }

    var node = {
      seq: len,
      feed: feed,
      key: key,
      path: path,
      value: value,
      clock: clock,
      trie: []
    }

    var missing = heads.length
    var error = null

    if (!missing) {
      missing++
      onput(null)
      return
    }

    for (i = 0; i < heads.length; i++) {
      self._visitPut(key, path, 0, 0, 0, heads[i], heads, node.trie, batch, onput)
    }

    function onput (err) {
      if (err) error = err
      if (--missing) return

      if (error) return cb(err)

      if (batch) {
        batch.push(node)
        return ondone(null, node)
      }

      self._localWriter.append(node, ondone)
    }

    function ondone (err) {
      if (err) return cb(err)
      return cb(null, self._map ? self._map(node) : node)
    }
  })
}

DB.prototype.path = function (key, cb) {
  var path = []
  this._get(key, false, null, onvisit, done)

  function onvisit (node) {
    path.push(node)
  }

  function done (err) {
    if (err) return cb(err)
    cb(null, path)
  }
}

DB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  this._get(key, !!(opts && opts.wait), [], noop, cb)
}

DB.prototype._get = function (key, wait, result, visit, cb) {
  var path = hash(key, true)
  var self = this
  var updates = this._updates

  this.heads(function (err, heads) {
    if (err) return cb(err)

    if (!heads.length) {
      if (wait) return self._wait(key, updates, result, visit, cb)
      return cb(null, null)
    }

    var missing = heads.length
    var error = null

    for (var i = 0; i < heads.length; i++) {
      self._visitGet(key, path, 0, heads[i], heads, result, visit, onget)
    }

    function onget (err) {
      if (err) error = err
      if (--missing) return
      if (error) return cb(error)

      if (!result || !result.length) {
        if (wait) return self._wait(key, updates, result, visit, cb)
        return cb(null, null)
      }

      if (self._map) result = result.map(self._map)
      if (self._reduce) result = result.reduce(self._reduce)

      cb(null, result)
    }
  })
}

DB.prototype._wait = function (key, oldUpdate, result, visit, cb) {
  if (oldUpdate !== this._updates) return this._get(key, true, result, visit, cb)
  this.once('remote-update', this._get.bind(this, key, true, result, visit, cb))
}

DB.prototype._visitPut = function (key, path, i, j, k, node, heads, trie, batch, cb) {
  var writers = this._writers
  var self = this
  var missing = 0
  var error = null
  var vals = null
  var remoteVals = null

  for (; i < path.length; i++) {
    var val = path[i]
    var local = trie[i]
    var remote = node.trie[i] || []

    // copy old trie
    for (; j < remote.length; j++) {
      if (j === val && val !== hash.TERMINATE) continue

      if (!local) local = trie[i] = []
      vals = local[j] = local[j] || []
      remoteVals = remote[j] || []

      for (; k < remoteVals.length; k++) {
        var rval = remoteVals[k]

        if (val === hash.TERMINATE) {
          getBatch(self, writers[rval.feed], batch, rval.seq, onfilterdups)
          return
        }

        if (noDup(vals, rval)) vals.push(rval)
      }
      k = 0
    }
    j = 0

    if (node.path[i] !== val || (node.path[i] === hash.TERMINATE && node.key !== key)) {
      // trie is splitting
      if (!local) local = trie[i] = []
      vals = local[node.path[i]] = local[node.path[i]] || []
      remoteVals = remote[val]
      vals.push({feed: node.feed, seq: node.seq})

      if (!remoteVals || !remoteVals.length) return cb(null)

      missing = remoteVals.length
      error = null

      for (var l = 0; l < remoteVals.length; l++) {
        getBatch(self, writers[remoteVals[l].feed], batch, remoteVals[l].seq, onremoteval)
      }
      return
    }
  }

  cb(null)

  function onfilterdups (err, val) {
    if (err) return cb(err)
    var valPointer = {feed: val.feed, seq: val.seq}
    if (val.key !== key && noDup(vals, valPointer)) vals.push(valPointer)
    self._visitPut(key, path, i, j, k + 1, node, heads, trie, batch, cb)
  }

  function onremoteval (err, val) {
    if (err) return onvisit(err)
    if (!updateHead(val, node, heads)) return onvisit(null)
    self._visitPut(key, path, i + 1, j, k, val, heads, trie, batch, onvisit)
  }

  function onvisit (err) {
    if (err) error = err
    if (!--missing) cb(error)
  }
}

function getBatch (self, w, batch, seq, cb) {
  if (!batch || self._localWriter !== w || seq < w.feed.length) return w.get(seq, cb)
  process.nextTick(cb, null, batch[seq - w.feed.length])
}

DB.prototype._open = function (cb) {
  var self = this
  var source = this._createFeed(this.key, 'source')

  source.on('ready', function () {
    self.source = source
    self.key = source.key
    self.discoveryKey = source.discoveryKey

    var w = self._createWriter(self.key, 'source')

    if (source.writable) {
      self.local = source
      self._localWriter = w
      return self._update(cb)
    }

    var local = self._createFeed(null, 'local')

    local.on('ready', function () {
      self.local = local
      self._update(cb)
    })
  })
}

DB.prototype._update = function (cb) {
  if (!cb) cb = noop

  var self = this
  var missing = this._writers.length
  var error = null
  var i = 0

  for (; i < this._writers.length; i++) {
    this._writers[i].head(done)
  }

  function done (err, head, updated) {
    if (err) error = err

    if (updated) {
      for (; i < self._writers.length; i++) {
        missing++
        self._writers[i].head(done)
      }
    }
    if (!--missing) cb(error)
  }
}

DB.prototype._createFeed = function (key, dir) {
  if (!dir) {
    dir = key.toString('hex')
    dir = 'peers/' + dir.slice(0, 2) + '/' + dir.slice(2)
  }

  if (key) {
    if (this.local && this.local.key && this.local.key.equals(key)) return this.local
    if (this.source && this.source.key && this.source.key.equals(key)) return this.source
  }

  var self = this
  var feed = hypercore(storage, key, {sparse: this.sparse})

  feed.on('error', onerror)
  feed.on('append', onappend)
  feed.on('download', ondownload)
  feed.on('upload', onupload)
  feed.on('remote-add', onremoteadd)
  feed.on('remote-update', onremoteupdate)
  feed.on('remote-remove', onremoteremove)

  return feed

  function onupload (index, data) {
    self.emit('upload', this, index, data)
  }

  function ondownload (index, data) {
    self.emit('download', this, index, data)
  }

  function onremoteupdate (peer) {
    self._updates++
    self.emit('remote-update', this, peer)
  }

  function onremoteadd (peer) {
    self.emit('remote-add', this, peer)
  }

  function onremoteremove (peer) {
    self.emit('remote-remove', this, peer)
  }

  function onerror (err) {
    self.emit('error', err)
  }

  function onappend () {
    self.emit('append', this)
  }

  function storage (name) {
    return self._storage(dir + '/' + name)
  }
}

DB.prototype._createWriter = function (key, dir) {
  for (var i = 0; i < this._writers.length; i++) {
    var w = this._writers[i]
    if (key && w.key.equals(key)) return w
  }

  var res = writer(this, this._createFeed(key, dir), this._writers.length)

  this._writers.push(res)
  this.emit('_writer', res)

  return res
}

DB.prototype._visitGet = function (key, path, i, node, heads, result, onvisit, cb) {
  var self = this
  var writers = this._writers
  var missing = 0
  var error = null
  var trie = null
  var vals = null

  for (; i < path.length; i++) {
    if (node.path[i] === path[i]) continue
    onvisit(node, i, true)

    // check trie
    trie = node.trie[i]
    if (!trie) return cb(null)

    vals = trie[path[i]]

    // not found
    if (!vals || !vals.length) return cb(null)

    missing = vals.length
    error = null

    for (var j = 0; j < vals.length; j++) {
      writers[vals[j].feed].get(vals[j].seq, onval)
    }

    return
  }

  // check for collisions
  trie = node.trie[path.length - 1]
  vals = trie && trie[hash.TERMINATE]

  pushMaybe(key, node, result, onvisit)

  if (!vals || !vals.length) return cb(null)

  missing = vals.length
  error = null

  for (var k = 0; k < vals.length; k++) {
    writers[vals[k].feed].get(vals[k].seq, onpush)
  }

  function onpush (err, val) {
    if (err) error = err
    else pushMaybe(key, val, result, onvisit)
    if (!--missing) cb(error)
  }

  function onval (err, val) {
    if (err) return done(err)

    if (!updateHead(val, node, heads)) {
      onvisit(val, i, false)
      done(null)
      return
    }

    self._visitGet(key, path, i + 1, val, heads, result, onvisit, done)
  }

  function done (err) {
    if (err) error = err
    if (!--missing) cb(error)
  }
}

DB.prototype.createDiffStream = function (key, checkout, head) {
  if (!checkout) checkout = []  // Diff from the beginning

  var stream = new Readable({objectMode: true})
  stream._read = noop

  function cb (err) {
    stream.emit('error', err)
  }

  var self = this
  var path = hash(key, true)
  var missing = 2

  var a = {}
  var b = {}

  // 1: Walk the trie starting at the head of all logs
  if (!head) this.heads(onHeads)
  else snapshotToNodes(head, onHeads)

  function onHeads (err, heads) {
    if (err) return cb(err)
    if (!heads.length) {
      return onDoneFromHead(null, {})
    }

    missing--
    var visited = {}
    for (var i = 0; i < heads.length; i++) {
      missing++
      self._visitTrie(key, path, heads[i], {}, checkout, visited, onDoneFromHead)
    }
  }

  // 2: Walk the trie starting at CHECKOUT
  snapshotToNodes(checkout, function (err, nodes) {
    if (err) return cb(err)
    if (!nodes.length) {
      return onDoneFromSnapshot(null, {})
    }
    missing--
    missing += nodes.length
    var visited = {}
    for (var i = 0; i < nodes.length; i++) {
      self._visitTrie(key, path, nodes[i], {}, null, visited, onDoneFromSnapshot)
    }
  })

  function snapshotToNodes (snapshot, cb) {
    cb = once(cb)
    var result = []
    var keys = Object.keys(snapshot || {})
    if (!keys.length) return cb(null, result)
    var pending = keys.length
    for (var i = 0; i < keys.length; i++) {
      var elm = snapshot[i]
      self._writers[i].get(elm, function (err, node) {
        if (err) return cb(err)
        result.push(node)
        if (!--pending) cb(null, result)
      })
    }
  }

  function onDoneFromHead (err, result) {
    if (err) return cb(err)
    merge(a, result)
    if (!--missing) onAllDone()
  }

  function onDoneFromSnapshot (err, result) {
    if (err) return cb(err)
    merge(b, result)
    if (!--missing) onAllDone()
  }

  // Merge b into a
  function merge (a, b) {
    Object.keys(b).forEach(function (key) {
      a[key] = (a[key] || []).concat(b[key])
    })
    return a
  }

  function onAllDone () {
    a = a || {}
    b = b || {}
    var diff = diffNodeSets(a, b)
    for (var i = 0; i < diff.length; i++) {
      stream.push(diff[i])
    }
    stream.push(null)
  }

  return stream
}

DB.prototype.snapshot = function (cb) {
  this.heads(function (err, heads) {
    if (err) return cb(err)
    if (!heads.length) return cb(null, [])

    var result = {}
    for (var i = 0; i < heads.length; i++) {
      result[heads[i].feed] = heads[i].seq
    }

    cb(null, result)
  })
}

DB.prototype._visitTrie = function (key, path, node, heads, halt, visited, cb, type) {
  var self = this
  var missing = 0

  cb = once(cb)

  var id = node.feed + ',' + node.seq
  visited[id] = true

  // We've traveled past 'snapshot' -- bail.
  if (halt && halt[node.feed] !== undefined && halt[node.feed] >= node.seq) {
    return cb(null, {})
  }

  // Check if this node matches the desired prefix.
  var prefixMatch = true
  for (var i = 0; i < path.length && path[i] !== hash.TERMINATE; i++) {
    if (path[i] !== node.path[i]) {
      prefixMatch = false
      break
    }
  }

  // Mark this match as either the first time we've seen it (heads), an older
  // value of this key we're still tracking backwards in time (snapshot), or a
  // duplicate that we've already procesed (deduped).
  if (prefixMatch && !heads[node.key]) {
    heads[node.key] = heads[node.key] || []
    heads[node.key].push(node)
  }

  // Traverse the node's entire trie, recursively, hunting for more nodes with
  // the desired prefix.
  for (var k = 0; k < node.trie.length; k++) {
    var trie = node.trie[k] || []
    for (i = 0; i < trie.length; i++) {
      var entrySet = trie[i] || []
      for (var j = 0; j < entrySet.length; j++) {
        var entry = entrySet[j]

        id = entry.feed + ',' + entry.seq
        if (visited[id]) continue
        visited[id] = true

        missing++
        self._writers[entry.feed].get(entry.seq, function (err, node) {
          if (err) return fin(null)
          self._visitTrie(key, path, node, heads, halt, visited, function (err) {
            if (err) return fin(err)
            if (!--missing) fin(null)
          })
        })
      }
    }
  }

  if (!missing) fin(null)

  // Finalize the results by taking a diff of 'heads' and 'snapshot'.
  function fin () {
    cb(null, heads)
  }
}

function noop () {}

function diffNodeSets (a, b) {
  var ak = Object.keys(a)
  var result = []
  for (var i = 0; i < ak.length; i++) {
    var A = a[ak[i]]
    var B = b[ak[i]]
    if (A && B && !entriesEqual(A, B)) {
      result.push({ type: 'del', name: ak[i], value: B.map(map) })
      result.push({ type: 'put', name: ak[i], value: A.map(map) })
    } else if (A && (!B || A === B)) {
      result.push({ type: 'put', name: ak[i], value: A.map(map) })
    }
  }
  return result

  function map (a) {
    return a.value
  }

  function entriesEqual (a, b) {
    if (a.length !== b.length) return false
    for (var i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false
    }
    return true
  }
}

function updateHead (newNode, oldNode, heads) {
  var i = heads.indexOf(oldNode)
  if (i !== -1) remove(heads, i)
  if (!isHead(newNode, heads)) return false
  heads.push(newNode)
  return true
}

function isHead (node, heads) {
  for (var i = 0; i < heads.length; i++) {
    var head = heads[i]
    if (head.feed === node.feed) return false
    if (node.seq < head.clock[node.feed]) return false
  }
  return true
}

function pushMaybe (key, node, results, onvisit) {
  onvisit(node, node.path.length, true)
  if (results && node.key === key && noDup(results, node)) results.push(node)
}

function noDup (list, val) {
  for (var i = 0; i < list.length; i++) {
    if (list[i].feed === val.feed && list[i].seq === val.seq) {
      return false
    }
  }
  return true
}

function isOptions (opts) {
  return !!(opts && typeof opts !== 'string' && !Buffer.isBuffer(opts))
}

function sortNodes (a, b) {
  if (a.feed === b.feed) return a.seq - b.seq
  return a.feed - b.feed
}

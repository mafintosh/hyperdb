var hash = require('./hash')
var writer = require('./writer')
var hypercore = require('hypercore')
var remove = require('unordered-array-remove')
var ram = require('random-access-memory')
var protocol = null // lazy load on replicate

var END_OF_PATH = 4 // max(hash alphabet) + 1

module.exports = DB

function DB (opts) {
  if (!(this instanceof DB)) return new DB(opts)
  if (!opts) opts = {}

  var self = this

  // TODO: automatically determine local id in writer.js
  this.id = opts.id || 0
  this.writers = []
  this.opened = false

  // TODO: remove me and change to do the same multi feed does
  if (opts.feed) opts.feeds = [opts.feed]
  if (!opts.feeds) opts.feeds = [hypercore(ram, {valueEncoding: 'json'})]
  for (var i = 0; i < opts.feeds.length; i++) {
    this.writers.push(writer(opts.feeds[i]))
  }

  this._hash = hash()
  this._map = opts.map || null
  this._reduce = opts.reduce || null

  this.ready() // call early
}

DB.prototype._heads = function (cb) {
  var result = []
  var missing = this.writers.length
  var error = null
  var self = this

  if (!missing) return process.nextTick(cb, null, result)

  this.ready(function (err) {
    if (err) return cb(err)
    for (var i = 0; i < self.writers.length; i++) {
      self.writers[i].head(onhead)
    }
  })

  function onhead (err, val) {
    if (err) error = err
    else if (val) result[val.log] = val
    if (--missing) return

    if (error) return cb(error)

    for (var i = 0; i < result.length; i++) {
      var head = result[i]
      if (!head) continue

      for (var j = 0; j < head.heads.length; j++) {
        if (result[j] && result[j].seq < head.heads[j]) result[j] = null
      }
    }

    cb(null, result.filter(Boolean))
  }
}

DB.prototype.authorize = function (key, id) {
  var feed = hypercore(key, ram)
  this.writers[id] = feed
}

DB.prototype.ready = function (cb) {
  // needs to be thunkyfied if it does more than call .ready

  if (!cb) cb = noop

  var self = this
  var missing = this.writers.length
  var error = null

  for (var i = 0; i < this.writers.length; i++) {
    this.writers[i].feed.ready(onready)
  }

  function onready (err) {
    if (err) error = err
    if (--missing) return
    if (!error) self.opened = true
    cb(error)
  }
}

DB.prototype.replicate = function (opts) {
  if (!protocol) protocol = require('hypercore-protocol')
  if (!opts) opts = {}

  opts.expectedFeeds = this.writers.length

  var self = this
  var stream = protocol(opts)

  opts.stream = stream

  this.ready(function (err) {
    if (err) return stream.destroy(err)
    if (stream.destroyed) return

    for (var i = 0; i < self.writers.length; i++) {
      self.writers[i].feed.replicate(opts)
    }
  })

  return stream
}

DB.prototype.put = function (key, value, cb) {
  if (!cb) cb = noop

  var log = this.id
  var self = this
  var path = this._hash(key).concat(END_OF_PATH)

  this._heads(function (err, h) {
    if (err) return cb(err)

    var newHeads = []
    for (var i = 0; i < self.writers.length; i++) {
      newHeads.push(i === log ? 0 : self.writers[i].feed.length)
    }

    if (!h.length) {
      self.writers[log].append({
        seq: self.writers[log].feed.length,
        log: log,
        key: key,
        path: path,
        value: value,
        heads: newHeads,
        trie: []
      }, cb)
      return
    }

    var trie = []
    var missing = h.length
    var error = null

    for (var i = 0; i < h.length; i++) {
      self._visitPut(key, path, 0, h[i], h, trie, onput)
    }

    function onput (err) {
      if (err) error = err
      if (--missing) return

      if (error) return cb(err)

      self.writers[log].append({
        seq: self.writers[log].feed.length,
        log: log,
        key: key,
        path: path,
        value: value,
        heads: newHeads,
        trie: trie
      }, cb)
    }
  })
}

DB.prototype.get = function (key, cb) {
  var path = this._hash(key).concat(END_OF_PATH)
  var result = []
  var self = this

  this._heads(function (err, h) {
    if (err) return cb(err)
    if (!h.length) return cb(null, null)

    var missing = h.length
    var error = null

    for (var i = 0; i < h.length; i++) {
      self._visitGet(key, path, 0, h[i], h, result, onget)
    }

    function onget (err) {
      if (err) error = err
      if (--missing) return
      if (error) return cb(error)

      if (!result.length) return cb(null, null)

      if (self._map) result = result.map(self._map)
      if (self._reduce) result = result.reduce(self._reduce)

      cb(null, result)
    }
  })
}

DB.prototype._visitPut = function (key, path, i, node, heads, trie, cb) {
  var writers = this.writers

  loop(i, 0, 0, cb)

  function loop (i, j, k, cb) {
    for (; i < path.length; i++) {
      var val = path[i]
      var local = trie[i]
      var remote = node.trie[i] || []

      // copy old trie
      for (; j < remote.length; j++) {
        if (j === val && val !== END_OF_PATH) continue

        if (!local) local = trie[i] = []
        var vals = local[j] = local[j] || []
        var remoteVals = remote[j] || []

        for (; k < remoteVals.length; k++) {
          var rval = remoteVals[k]

          if (val === END_OF_PATH) {
            // TODO: used to be node.key which is prob a bug
            writers[rval.log].get(rval.seq, function (err, val) {
              if (err) return cb(err)
              if (val.key !== key && noDup(vals, rval)) vals.push(rval)
              loop(i, j, k + 1, cb)
            })
            return
          }

          if (noDup(vals, rval)) vals.push(rval)
        }
        k = 0
      }
      j = 0

      if (node.path[i] !== val || (node.path[i] === END_OF_PATH && node.key !== key)) {
        // trie is splitting
        if (!local) local = trie[i] = []
        var vals = local[node.path[i]] = local[node.path[i]] || []
        var remoteVals = remote[val]

        vals.push({log: node.log, seq: node.seq})

        if (!remoteVals || !remoteVals.length) return cb(null)

        if (remoteVals.length === 1) {
          writers[remoteVals[0].log].get(remoteVals[0].seq, function (err, val) {
            if (err) return cb(err)
            if (!updateHead(val, node, heads)) return cb(null)
            node = val
            loop(i + 1, j, k, cb)
          })
          return
        }

        var missing = remoteVals.length
        var error = null

        for (var l = 0; l < remoteVals.length; l++) {
          writers[remoteVals[l].log].get(remoteVals[l].seq, onremoteval)
        }

        function onremoteval (err, val) {
          if (err) return onvisit(err)
          if (!updateHead(val, node, heads)) return onvisit(null)
          self._visitPut(key, path, i + 1, val, heads, trie, onvisit)
        }

        function onvisit (err) {
          if (err) error = err
          if (!--missing) cb(error)
        }
        return
      }
    }

    cb(null)
  }
}

DB.prototype._visitGet = function (key, path, i, node, heads, result, cb) {
  var self = this
  var writers = this.writers

  for (; i < path.length; i++) {
    if (node.path[i] === path[i]) continue

    // check trie
    var trie = node.trie[i]
    if (!trie) return cb(null)

    var vals = trie[path[i]]

    // not found
    if (!vals || !vals.length) return cb(null)

    var missing = vals.length
    var error = null

    for (var j = 0; j < vals.length; j++) {
      writers[vals[j].log].get(vals[j].seq, onval)
    }

    function onval (err, val) {
      if (err) return done(err)
      if (!updateHead(val, node, heads)) return done(null)
      self._visitGet(key, path, i + 1, val, heads, result, done)
    }

    function done (err) {
      if (err) error = err
      if (!--missing) cb(error)
    }

    return
  }

  // check for collisions
  var trie = node.trie[path.length - 1]
  var vals = trie && trie[END_OF_PATH]

  pushMaybe(key, node, result)

  if (!vals || !vals.length) return cb(null)

  var missing = vals.length
  var error = null

  for (var i = 0; i < vals.length; i++) {
    writers[vals[i].log].get(vals[i].seq, onpush)
  }

  function onpush (err, val) {
    if (err) error = err
    else pushMaybe(key, val, result)
    if (!--missing) cb(error)
  }
}

function noop () {}

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
    if (head.log === node.log) return false
    if (node.seq < head.heads[node.log]) return false
  }
  return true
}

function pushMaybe (key, node, results) {
  if (node.key === key && noDup(results, node)) results.push(node)
}

function noDup (list, val) {
  for (var i = 0; i < list.length; i++) {
    if (list[i].log === val.log && list[i].seq === val.seq) {
      // console.log('ignore dup', val)
      return false
    }
  }
  return true
}

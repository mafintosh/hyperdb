var unordered = require('unordered-set')
var util = require('util')
var events = require('events')

module.exports = watch

function watch (db, key, cb) {
  var w = new Watcher(db, key)
  w._index = db._watching.push(w) - 1
  w.start(cb)
  return w
}

function Watcher (db, key) {
  events.EventEmitter.call(this)

  this.key = key

  this._index = 0
  this._db = db
  this._kicked = 0
  this._nodes = null
  this._destroyed = false
  this._onkick = onkick.bind(this)
}

util.inherits(Watcher, events.EventEmitter)

Watcher.prototype.destroy = function (err) {
  if (this._destroyed) return
  this._destroyed = true

  unordered.remove(this._db._watching, this)
  if (err) this.emit('error', err)
  this.emit('close')
}

Watcher.prototype.start = function (onchange) {
  if (onchange) this.on('change', onchange)
  this._kick()
}

Watcher.prototype._kick = function () {
  this._kicked++
  this._db.get(this.key, {prefix: true, map: false, reduce: false}, this._onkick)
}

function same (a, b) {
  if (a.length !== b.length) return false

  for (var i = 0; i < a.length; i++) {
    if (a[i].feed !== b[i].feed || a[i].seq !== b[i].seq) return false
  }

  return true
}

function sortByFeed (a, b) {
  return a.feed - b.feed
}

function onkick (err, nodes) {
  if (err) return this.destroy(err)

  var kicked = this._kicked

  this._kicked = 0
  nodes = nodes.sort(sortByFeed)

  if (!this._nodes) {
    this._nodes = nodes
    this.emit('watching')
  }

  if (!same(nodes, this._nodes)) {
    this._nodes = nodes
    this.emit('change')
    return
  }

  // there is a chance the db has been updated while we
  // ran the query - retry
  if (kicked > 1) this._kick()
}

var unordered = require('unordered-set')

module.exports = watch

function watch (db, key, cb) {
  var w = new Watcher(db, key)
  w._id = db._watching.push(w) - 1
  w.start(cb)
  return w
}

function Watcher (db, key) {
  this.key = key
  this._id = 0
  this._db = db
  this._kicked = 0
  this._nodes = null
  this._onchange = null
  this._onkick = onkick.bind(this)
}

Watcher.prototype.destroy = function (err) {
  if (!this._onchange) return

  var cb = this._onchange
  this._onchange = null

  unordered.remove(this._db._watching, this)

  cb(err)
}

Watcher.prototype.start = function (cb) {
  this._onchange = cb
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
  if (err) return this._destroy(err)
  if (!this._onchange) return

  var kicked = this._kicked

  this._kicked = 0
  nodes = nodes.sort(sortByFeed)
  if (!this._nodes) this._nodes = nodes

  if (!same(nodes, this._nodes)) {
    this._nodes = nodes
    this._onchange(null)
    return
  }

  // there is a chance the db has been updated while we
  // ran the query - retry
  if (kicked > 1) this._kick()
}

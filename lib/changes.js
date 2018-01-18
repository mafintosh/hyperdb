module.exports = Changes

function Changes (db) {
  if (!(this instanceof Changes)) return new Changes(db)
  this._db = db
  this._bottom = []
  this._clock = []
}

Changes.prototype._synced = function () {
  return this._bottom.length === this._db._feeds.length
}

Changes.prototype._syncAndNext = function (cb) {
  var self = this

  update()

  function update () {
    if (self._synced()) {
      self.next(cb)
    } else {
      self._db._getPointer(self._bottom.length, 0, next)
    }
  }

  function next (err, val) {
    if (err) return cb(err)
    self._bottom.push(val)
    self._clock.push(0)
    update()
  }
}

Changes.prototype.next = function (cb) {
  if (!this._synced()) {
    this._syncAndNext(cb)
    return
  }

  var min = this._min()
  if (min === -1) return process.nextTick(cb, null, null)

  var self = this
  var node = this._bottom[min]
  var next = node.seq + 1

  this._get(min, next, done)

  function done (err, nextNode) {
    if (err) return cb(err)
    self._bottom[min] = nextNode
    cb(null, node)
  }
}

Changes.prototype._get = function (feedId, seq, cb) {
  var self = this
  var len = this._db._feeds[feedId].length

  if (len <= seq) {
    this._clock[feedId] = len
    process.nextTick(cb, null, null)
    return
  }
  
  this._db._getPointer(feedId, seq, resolve)

  function resolve (err, node) {
    if (err) return cb(err)
    
    // TODO: set a flag if we should even enter this loop
    for (var i = 0; i < node.clock.length; i++) {
      if (self._bottom[i] || self._clock[i] >= node.clock[i]) continue
      return self._update(i, node, cb)
    }

    cb(null, node)
  }
}

Changes.prototype._update = function (i, node, cb) {
  var missing = 0
  var error = null

  for (; i < node.clock.length; i++) {
    if (this._bottom[i] || this._clock[i] >= node.clock[i]) continue
    missing++
    this._db._getPointer(i, this._clock[i], done)
  }

  function done (err, dep) {
    if (err) error = err
    else self._bottom[dep.feed] = dep
    if (--missing) return
    cb(error, node)
  }
}

Changes.prototype._min = function () {
  var i = -1

  for (var j = 0; j < this._bottom.length; j++) {
    var node = this._bottom[j]
    if (!node) continue
    var min = i > -1 && this._bottom[i]
    if (!min || (min.feed < node.clock.length &&  min.seq >= node.clock[min.feed])) i = j
  }

  return i
}

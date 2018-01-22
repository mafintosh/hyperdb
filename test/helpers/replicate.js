module.exports = function (a, b, cb) {
  if (b._feeds[b._id]) a._feeds[b._id] = b._feeds[b._id].slice(0)
  if (a._feeds[a._id]) b._feeds[a._id] = a._feeds[a._id].slice(0)
  process.nextTick(cb)
}

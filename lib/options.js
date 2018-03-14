exports.map = function (opts, db) {
  if (!opts) return db._map
  var map = opts.map
  return map === undefined ? db._map : map
}

exports.reduce = function (opts, db) {
  if (!opts) return db._reduce
  var reduce = opts.reduce
  return reduce === undefined ? db._reduce : reduce
}

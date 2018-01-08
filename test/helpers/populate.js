module.exports = populate

function promisedPut (db, key, val) {
  return new Promise((resolve, reject) => db.put(key, val, (e) => {
    if (e) return reject(e)
    resolve()
  }))
}

function populate (db, vals, offset, cb) {
  var promised = vals.reduce((p, v, i) => {
    return p.then(() => promisedPut(db, v, (offset || 0) + i))
  }, Promise.resolve())
  promised.then(cb).catch(e => cb(e))
}

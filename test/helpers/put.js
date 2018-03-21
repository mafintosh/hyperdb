module.exports = function (db, list, cb) {
  var i = 0
  loop(null)

  function loop (err) {
    if (err) return cb(err)
    if (i === list.length) return cb(null)

    var next = list[i++]
    if (typeof next === 'string') next = {key: next, value: next}
    console.log('PUTTING', next.value, 'AT KEY:', next.key)
    db.put(next.key, next.value, loop)
  }
}

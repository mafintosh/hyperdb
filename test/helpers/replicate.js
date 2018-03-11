module.exports = replicate

function replicate (a, b, opts, cb) {
  if (typeof opts === 'function') return replicate(a, b, null, opts)

  var s1 = a.replicate(opts)
  var s2 = b.replicate(opts)

  s1.pipe(s2).pipe(s1).on('end', function () {
    if (cb) cb()
  })
}

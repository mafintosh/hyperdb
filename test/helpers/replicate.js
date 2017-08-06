module.exports = replicate

function replicate (a, b, cb) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
}

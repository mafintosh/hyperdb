module.exports = run

function run () {
  var fns = [].concat.apply([], arguments) // flatten
  loop(null)

  function loop (err) {
    if (fns.length === 1 || err) return fns.pop()(err)
    fns.shift()(loop)
  }
}

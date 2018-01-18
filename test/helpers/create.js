exports.one = function () {
  return require('../../')({id: 0, reduce: (a, b) => a})
}

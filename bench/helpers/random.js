var random = require('random-seed')
var from = require('from2')

function makeDefault (opts) {
  return Object.assign({
    seed: 1,
    valueSize: 10,
    keyLength: 5,
    numKeys: 1e3
  }, opts)
}

module.exports.fullData = function (opts) {
  opts = makeDefault(opts)
  var rand = random.create(opts.seed)
  var data = []
  for (var i = 0; i < opts.numKeys; i++) {
    var val = {
      type: 'put',
      key: rand.string(opts.keyLength),
      value: rand.string(opts.valueSize)
    }
    data.push(val)
  }
  return data
}

module.exports.streamingData = function (opts) {
  opts = makeDefault(opts)
  var rand = random.create(opts.seed)
  return from.obj(function (size, next) {
    next(null, {
      type: 'put',
      key: String(rand.intBetween(opts.minKey, opts.maxKey)),
      value: rand.string(opts.valueSize)
    })
  })
}

function multiply (x, y) { return x * y }

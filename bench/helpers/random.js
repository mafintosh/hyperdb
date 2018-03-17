var random = require('random-seed')
var from = require('from2')

function makeDefault (opts) {
  return Object.assign({
    seed: 1,
    valueSize: 10,
    maxKeyLength: 20,
    numKeys: 1e5,
    minKey: 1,
    maxKey: 1e4
  }, opts)
}

module.exports.fullData = function (opts) {
  opts = makeDefault(opts)
  console.log('OPTS:', opts)
  var rand = random.create(opts.seed)
  var data = []
  for (var i = 0; i < opts.numKeys; i++) {
    data.push({
      type: 'put',
      key: String(rand.intBetween(opts.minKey, opts.maxKey)),
      value: rand.string(opts.valueSize)
    })
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

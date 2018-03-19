var random = require('random-seed')
var from = require('from2')

var rand = random.create('hello')

// 10 possible key characters.
var STRING_CHARS = ['a','b','c','d','e','f','g','h','i','j']

module.exports.data = function (opts) {
  opts = makeDefault(opts)
  var data = []
  for (var i = 0; i < opts.numKeys; i++) {
    var val = {
      type: 'put',
      key: randomString(5),
      value: rand.string(opts.valueSize)
    }
    data.push(val)
  }
  return data
}

module.exports.string = randomString

function randomString (length) {
  var string = ''
  for (var i = 0; i < length; i++) {
    string += STRING_CHARS[rand.intBetween(0, STRING_CHARS.length - 1)]
  }
  return string
}

function makeDefault (opts) {
  return Object.assign({
    valueSize: 10,
    keyLength: 5,
    numKeys: 1e3
  }, opts)
}

function multiply (x, y) { return x * y }

var p = require('path')
var fs = require('fs')

var prettier = require('prettier')
var seed = require('seed-random')

var normalizeKey = require('../../lib/normalize')
var create = require('./create')
var run = require('./run')
var put = require('./put')

const ALPHABET = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

module.exports.validate = validate
module.exports.fuzzRunner = fuzzRunner

/**
 * Fuzzing tests can be specified using an options dictionary with this form:
 * (Defaults specified in the example below).
 *
 * {
 *   keys: 1000, // The total number of records.
 *   writers: 1, // The total number of writers.
 *   keyDepth: 1, // The maximum number of path components per key.
 *   dirs: 20, // The approximate number of directories that will be created.
 *   dirSize: 10, // The approximate number of keys at each prefix level.
 *   prefixSize: 5, // The size of each path component.
 *   conflicts: 0, // The approximate number of conflicting keys.
 *   replications: 10, // The approximate number of all-to-all replications/tests.
 *   valueSize: 20, // The size of each value.
 *   seed: 'hello' // The seed (for repeatable testing).
 * }
 */
function defaultOpts (opts) {
  return Object.assign({
    keys: 1000,
    writers: 1,
    keyDepth: 2,
    dirs: 20,
    dirSize: 10,
    prefixSize: 5,
    conflicts: 0,
    replications: 10,
    valueSize: 20,
    seed: 'hello'
  }, opts || {})
}

function test (probability, rand) {
  return rand() < probability
}

function sample (arr, count, rand, withReplacement) {
  if (count > arr.length && !withReplacement) throw Error('Invalid sampling arguments.')
  var result = []
  while (result.length !== count) {
    var candidate = arr[Math.floor(rand() * arr.length)]
    if (withReplacement) result.push(candidate)
    else if (result.indexOf(candidate) === -1) result.push(candidate)
  }
  return result
}

function makeDatabases (opts, cb) {
  create.many(opts.writers, function (err, dbs, replicateByIndex) {
    if (err) throw err
    return cb(dbs, replicateByIndex)
  })
}

function generateData (opts) {
  var random = seed(opts.seed)

  var keysPerReplication = []
  var writesPerReplication = []
  var writers = new Array(opts.writers).fill(0).map((_, i) => i)

  // Generate the list of all keys that will be inserted.
  var stack = []
  for (var i = 0; i < opts.keys; i++) {
    var prefix = sample(ALPHABET, opts.prefixSize, random, true).join('')

    var shouldPushDir = test(opts.dirs / opts.keys, random) &&
      stack.length < opts.keyDepth
    var shouldPopDir = stack.length && test(1 / opts.dirSize, random)
    var shouldReplicate = test(opts.replications / opts.keys, random)

    if (shouldPushDir) stack.push(prefix)
    if (shouldPopDir) stack.pop()

    var batchIdx = (!keysPerReplication.length) ? 0 : keysPerReplication.length - 1

    if (!keysPerReplication[batchIdx]) keysPerReplication.push([])

    keysPerReplication[batchIdx].push(normalizeKey(stack.join('/') + '/' + prefix))
    if (shouldReplicate) keysPerReplication.push([])
  }

  // Generate the values for those keys (including possible conflicts).
  for (i = 0; i < keysPerReplication.length; i++) {
    var keyBatch = keysPerReplication[i]
    var writeBatch = []
    for (var j = 0; j < keyBatch.length; j++) {
      var singleWrite = { key: keyBatch[j], values: [] }
      var shouldConflict = opts.conflicts && test(opts.conflicts / opts.keys, random)
      var keyWriters = null
      if (shouldConflict) {
        var numConflicts = Math.floor(random() * opts.writers) + 1
        keyWriters = sample(writers, numConflicts, random, false)
      } else {
        keyWriters = sample(writers, 1, random, false)
      }
      for (var z = 0; z < keyWriters.length; z++) {
        var valueString = sample(ALPHABET, opts.valueSize, random, true).join('')
        singleWrite.values[keyWriters[z]] = valueString
      }
      writeBatch.push(singleWrite)
    }
    writesPerReplication.push(writeBatch)
  }

  return writesPerReplication
}

function validate (t, db, processedBatches, cb) {
  var expectedWrites = {}
  // Assuming the batches are insertion order.
  for (var i = 0; i < processedBatches.length; i++) {
    var writeBatch = processedBatches[i]
    for (var j = 0; j < writeBatch.length; j++) {
      var singleWrite = writeBatch[j]
      expectedWrites[singleWrite.key] = singleWrite.values.filter(val => !!val)
    }
  }
  var allKeys = Object.keys(expectedWrites)

  t.test(`validating after ${processedBatches.length} replications`, function (t) {
    t.plan(allKeys.length + 1)

    var readStream = db.createReadStream('/')
    readStream.on('end', function () {
      var keys = Object.keys(expectedWrites)
      t.same(keys.length, 0, 'missing keys: ' + keys)
      if (keys.length === 0) return cb()
      return cb(new Error(`missing keys: ${keys}`))
    })
    readStream.on('error', cb)
    readStream.on('data', function (nodes) {
      if (!nodes) return
      var key = nodes[0].key
      var values = nodes.map(node => node.value)
      t.same(values, expectedWrites[key])
      delete expectedWrites[key]
    })
  })
}

function generateFailingTest (name, dbCount, writesPerReplication, writeOps, cb) {
  writeOps.push(err => {
    t.error(err)
    t.end()
  }).toString()
  console.log('\n Generated Test Case:\n')
  console.log(prettier.format(`
    var tape = require('tape')  

    var run = require('./helpers/run')
    var put = require('./helpers/put')
    var create = require('./helpers/create')

    var validate = require('./helpers/fuzzing').validate

    var writesPerReplication = ${JSON.stringify(writesPerReplication)}

    tape('autogenerated failing fuzz test', function (t) {
      create.many(${dbCount}, function (err, dbs, replicateByIndex) {
        t.error(err)
        run(${writeOps.map(op => op.toString())})
      })
    })`, { singleQuote: true, semi: false }))
  console.log('\n')
  process.nextTick(cb)
}

var testNum = 0

function fuzzRunner (t, opts, cb) {
  opts = defaultOpts(opts)

  // Used for keeping track of failing test cases.
  testNum++

  var writesPerReplication = generateData(opts)

  makeDatabases(opts, function (dbs, replicateByIndex) {
    var ops = []
    for (var i = 0; i < writesPerReplication.length; i++) {
      var batch = writesPerReplication[i]
      var batchOps = []
      for (var j = 0; j < batch.length; j++) {
        var singleWrite = batch[j]
        for (var z = 0; z < singleWrite.values.length; z++) {
          var value = singleWrite.values[z]
          if (!value) continue
          batchOps.push(
            // Evaling here so that function.toString contains variable values.
            // (Used for test code generation).
            eval(`(cb => {
              put(dbs[${z}], [{
                key: '${singleWrite.key}',
                value: '${value}'
              }], cb)
            })`)
          )
        }
      }
      ops.push(batchOps)
      // Intersperse replication/validation/failing-test generation between write batches.
      ops.push([
        // Currently replicating between all databases at every replication point.
        cb => replicateByIndex(cb),
        // Evaling to capture `i` for test generation.
        eval(`(cb => validate(t, dbs[0], writesPerReplication.slice(0, ${i + 1}), cb))`)
      ])
    }

    var finished = 0
    doRun()

    function doRun (err) {
      if (err) {
        // Don't include the validation/replication ops in the test case generation.
        // Also don't include the doRun callback in each batch
        var failingBatches = ops.slice(0, finished).map(batch => batch.slice(0, -1))
        generateFailingTest(`failing-test-${testNum}`, opts.writers, writesPerReplication, failingBatches, () => {
          return cb(err)
        })
        return
      } else {
        if (finished === ops.length) return cb(null)
      }
      ops[finished].push(doRun)
      run.apply(null, ops[finished++])
    }
  })
}

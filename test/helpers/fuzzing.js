var prettier = require('prettier')
var standard = require('standard')
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
    var writeBatch = new Map()
    for (var j = 0; j < keyBatch.length; j++) {
      var shouldConflict = opts.conflicts && test(opts.conflicts / opts.keys, random)
      var keyWriters = null

      var numConflicts = shouldConflict ? Math.floor(random() * opts.writers) + 1 : 1
      keyWriters = sample(writers, numConflicts, random, false)

      var values = []
      for (var z = 0; z < keyWriters.length; z++) {
        var valueString = sample(ALPHABET, opts.valueSize, random, true).join('')
        values[keyWriters[z]] = valueString
      }

      writeBatch.set(keyBatch[j], values)
    }
    writesPerReplication.push(writeBatch)
  }

  return writesPerReplication
}

function validate (t, db, processedBatches, cb) {
  var expectedWrites = new Map()
  for (var i = 0; i < processedBatches.length; i++) {
    processedBatches[i].forEach((v, k) => expectedWrites.set(k, v))
  }

  t.test(`validating after ${processedBatches.length} replications`, function (t) {
    t.plan(expectedWrites.size + 1)

    var readStream = db.createReadStream('/')
    readStream.on('end', function () {
      var keys = expectedWrites.size === 0 ? 'none' : Array.from(expectedWrites.keys()).join(',') 
      t.same(expectedWrites.size, 0, `missing keys: ${keys}`)

      if (expectedWrites.size === 0) return cb()
      return cb(new Error(`missing keys: ${keys}`))
    })
    readStream.on('error', cb)
    readStream.on('data', function (nodes) {
      if (!nodes) return
      var key = nodes[0].key
      var values = nodes.map(node => node.value)
      t.same(values, expectedWrites.get(key).filter(v => !!v))
      expectedWrites.delete(key)
    })
  })
}

function generateFailingTest (dbCount, writesPerReplication, writeOps) {
  writeOps.push(err => {
    t.error(err)
    t.end()
  }).toString()

  var writeArrays = writesPerReplication.map(m => Array.from(m))

  console.log('\n Generated Test Case:\n')
  var source = prettier.format(`
    var tape = require('tape')  

    var run = require('./helpers/run')
    var put = require('./helpers/put')
    var create = require('./helpers/create')
    var validate = require('./helpers/fuzzing').validate

    tape('autogenerated failing fuzz test', function (t) {
      var writesPerReplication = ${JSON.stringify(writeArrays)}.map(b => new Map(b))

      create.many(${dbCount}, function (err, dbs, replicateByIndex) {
        t.error(err)
        run(${writeOps.map(op => op.toString())})
      })
    })`, { singleQuote: true, semi: false })

  var standardized = standard.lintTextSync(source, { fix: true })
  console.log(standardized.results[0].output)
  console.log('\n')
}

function fuzzRunner (t, opts, cb) {
  opts = defaultOpts(opts)

  var writesPerReplication = generateData(opts)

  makeDatabases(opts, function (dbs, replicateByIndex) {
    var ops = []
    for (var i = 0; i < writesPerReplication.length; i++) {
      var batch = writesPerReplication[i]
      var batchOps = []
      for (var b of batch) {
        var key = b[0]
        var values = b[1]
        for (var j = 0; j < values.length; j++) {
          var value = values[j]
          if (!value) continue
          batchOps.push(
            // Evaling here so that function.toString contains variable values.
            // (Used for test code generation).
            eval(`(cb => {
              put(dbs[${j}], [{
                key: '${key}',
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
        generateFailingTest(opts.writers, writesPerReplication, failingBatches)
        return cb(null)
      } else if (finished === ops.length) {
        return cb(null)
      }
      ops[finished].push(doRun)
      run.apply(null, ops[finished++])
    }
  })
}

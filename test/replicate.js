var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')

var hyperdb = require('..')
var hypercore = require('hypercore')
var ram = require('random-access-memory')

tape('basic replication', function (t) {
  t.plan(6)

  createTwo(function (a, b) {
    a.put('/a', 'a', function () {
      replicate(a, b, validate)
    })

    function validate () {
      b.get('/a', function (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes[0].key, '/a')
        t.same(nodes[0].value, 'a')
      })

      b.get('/a', function (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes[0].key, '/a')
        t.same(nodes[0].value, 'a')
      })
    }
  })
})

tape('2 peers, fork', function (t) {
  t.plan(20)

  createTwo(function (a, b) {
    a.put('/root', 'root', function () {
      replicate(a, b, function () {
        a.put('/key', 'a', function () {
          b.put('/key', 'b', function () {
            replicate(a, b, validate)
          })
        })
      })
    })

    function validate () {
      a.get('/root', ongetroot)
      b.get('/root', ongetroot)
      a.get('/key', ongetkey)
      b.get('/key', ongetkey)

      function ongetroot (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes.length, 1)
        t.same(nodes[0].key, '/root')
        t.same(nodes[0].value, 'root')
      }

      function ongetkey (err, nodes) {
        t.error(err, 'no error')
        nodes.sort(sort)
        t.same(nodes.length, 2)
        t.same(nodes[0].key, '/key')
        t.same(nodes[0].value, 'a')
        t.same(nodes[1].key, '/key')
        t.same(nodes[1].value, 'b')
      }
    }
  })
})

tape('2 peers, fork and non-merge write', function (t) {
  t.plan(20)

  createTwo(function (a, b) {
    a.put('/root', 'root', function () {
      replicate(a, b, function () {
        a.put('/key', 'a', function () {
          b.put('/key', 'b', function () {
            replicate(a, b, function () {
              a.put('/root', 'new root', function () {
                replicate(a, b, validate)
              })
            })
          })
        })
      })
    })

    function validate () {
      a.get('/root', ongetroot)
      b.get('/root', ongetroot)
      a.get('/key', ongetkey)
      b.get('/key', ongetkey)

      function ongetroot (err, nodes) {
        // console.log('-->', nodes)
        t.error(err, 'no error')
        t.same(nodes.length, 1)
        t.same(nodes[0].key, '/root')
        t.same(nodes[0].value, 'new root')
      }

      function ongetkey (err, nodes) {
        t.error(err, 'no error')
        nodes.sort(sort)
        t.same(nodes.length, 2)
        t.same(nodes[0].key, '/key')
        t.same(nodes[0].value, 'a')
        t.same(nodes[1].key, '/key')
        t.same(nodes[1].value, 'b')
      }
    }
  })
})

tape.skip('2 peers, 1 reference old value', function (t) {
  t.plan(16)

  createTwo(function (a, b) {
    a.put('/a', 'old', function () {
      replicate(a, b, function () {
        a.put('/a', 'new', function () {
          a.put('/foo', 'meh', function () {
            b.put('/other', 'meh', function () {
              replicate(a, b, validate)
            })
          })
        })
      })
    })

    function validate () {
      a.get('/a', function (err, nodes) {
        console.log(nodes)
      })
    }
  })
})

tape('2 peers, fork and merge write', function (t) {
  t.plan(16)

  createTwo(function (a, b) {
    a.put('/root', 'root', function () {
      replicate(a, b, function () {
        a.put('/key', 'a', function () {
          b.put('/key', 'b', function () {
            replicate(a, b, function () {
              a.put('/key', 'c', function () {
                replicate(a, b, validate)
              })
            })
          })
        })
      })
    })

    function validate () {
      a.get('/root', ongetroot)
      b.get('/root', ongetroot)
      a.get('/key', ongetkey)
      b.get('/key', ongetkey)

      function ongetroot (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes.length, 1)
        t.same(nodes[0].key, '/root')
        t.same(nodes[0].value, 'root')
      }

      function ongetkey (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes.length, 1)
        t.same(nodes[0].key, '/key')
        t.same(nodes[0].value, 'c')
      }
    }
  })
})

function sort (a, b) {
  return a.value.toString('hex').localeCompare(b.value.toString('hex'))
}

function createTwo (cb) {
  var a = hypercore(ram, {valueEncoding: 'json'})
  var b = hypercore(ram, {valueEncoding: 'json'})
  var dbs = {}

  a.ready(function () {
    b.ready(function () {
      cb(
        hyperdb({feeds: [a, hypercore(ram, b.key, {valueEncoding: 'json'})], id: 0}),
        hyperdb({feeds: [hypercore(ram, a.key, {valueEncoding: 'json'}), b], id: 1})
      )
    })
  })
}

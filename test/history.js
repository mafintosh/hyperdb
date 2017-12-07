var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('empty history', function (t) {
  var db = create.one()

  var expected = []

  var rs = db.createHistoryStream()
  collect(rs, function (err, actual) {
    t.error(err, 'no error')
    t.deepEqual(actual, expected, 'diff as expected')
    t.end()
  })
})

tape('single value', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createHistoryStream()
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 1)
      t.equals(actual[0].key, '/a')
      t.equals(actual[0].value, '2')
      t.end()
    })
  })
})

tape('multiple values', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/b/0', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, '/a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, '/b/0')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('multiple values: same key', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/a', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, '/a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, '/a')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('2 feeds', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      b.put('/b', '12', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 3)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '1,0')
        t.equals(actual[2].feed + ',' + actual[2].seq, '0,1')
        t.end()
      })
    }
  })
})

tape('2 feeds: clock conflict', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        replicate(a, b, function () {
          a.put('/a', 'c', function () {
            replicate(a, b, function () {
              validate()
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 4)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '1,0')
        t.equals(actual[2].feed + ',' + actual[2].seq, '0,1')
        t.equals(actual[3].feed + ',' + actual[3].seq, '0,2')
        t.end()
      })
    }
  })
})

tape('3 feeds', function (t) {
  create.three(function (a, b, c) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        c.put('/a', 'c', function () {
          replicate(a, b, function () {
            replicate(a, c, function () {
              validate()
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 5)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '1,0')
        t.equals(actual[2].feed + ',' + actual[2].seq, '2,0')
        t.equals(actual[3].feed + ',' + actual[3].seq, '1,1')
        t.equals(actual[4].feed + ',' + actual[4].seq, '0,1')
        t.end()
      })
    }
  })
})

tape('3 feeds: clock conflict', function (t) {
  create.three(function (a, b, c) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        replicate(a, b, function () {
          c.put('/a', 'c', function () {
            replicate(a, c, function () {
              c.put('/a', 'd', function () {
                validate()
              })
            })
          })
        })
      })
    })

    function validate () {
      var rs = c.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 6)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '1,0')
        t.equals(actual[2].feed + ',' + actual[2].seq, '2,0')
        t.equals(actual[3].feed + ',' + actual[3].seq, '2,1')
        t.equals(actual[4].feed + ',' + actual[4].seq, '0,1')
        t.equals(actual[5].feed + ',' + actual[5].seq, '1,1')
        t.end()
      })
    }
  })
})

tape('1 feed: start version', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.version(function (err, version) {
      t.error(err, 'no error')
      db.put('/b/0', 'boop', function (err) {
        t.error(err, 'no error')
        var rs = db.createHistoryStream({start: version})
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 1)
          t.equals(actual[0].key, '/b/0')
          t.equals(actual[0].value, 'boop')
          t.end()
        })
      })
    })
  })
})

tape('2 feeds: start version', function (t) {
  var start1 = null
  var start2 = null

  create.two(function (a, b) {
    a.put('/a', 'a', function (err) {
      t.ifError(err)
      b.put('/a', 'b', function (err) {
        t.ifError(err)
        a.version(function (err, version) {
          t.ifError(err)
          start1 = version
          replicate(a, b, function () {
            a.version(function (err, version) {
              t.ifError(err)
              start2 = version
              a.put('/a', 'c', function (err) {
                t.ifError(err)
                replicate(a, b, function () {
                  validate()
                })
              })
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream({start: start1})
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].feed + ',' + actual[0].seq, '1,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,2')

        var rs = a.createHistoryStream({start: start2})
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 1)
          t.equals(actual[0].feed + ',' + actual[0].seq, '0,2')
          t.end()
        })
      })
    }
  })
})

tape('3 feeds: start version', function (t) {
  var start1 = null
  var start2 = null
  var start3 = null

  create.three(function (a, b, c) {
    a.put('/a', 'a', function (err) {
      t.ifError(err)
      b.put('/a', 'b', function (err) {
        t.ifError(err)
        a.version(function (err, version) {
          t.ifError(err)
          start1 = version
          c.put('/a', 'c', function (err) {
            t.ifError(err)
            replicate(a, b, function () {
              b.version(function (err, version) {
                t.ifError(err)
                start2 = version
                a.put('/a', 'd', function (err) {
                  t.ifError(err)
                  replicate(a, c, function () {
                    c.version(function (err, version) {
                      t.ifError(err)
                      start3 = version
                      validate()
                    })
                  })
                })
              })
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream({start: start1})
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 4)
        t.equals(actual[0].feed + ',' + actual[0].seq, '1,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '2,0')
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,1')
        t.equals(actual[3].feed + ',' + actual[3].seq, '0,2')

        var rs = b.createHistoryStream({start: start2})
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 0)

          var rs = c.createHistoryStream({start: start3})
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.equals(actual.length, 1)
            t.equals(actual[0].feed + ',' + actual[0].seq, '1,0')
            t.end()
          })
        })
      })
    }
  })
})

tape('live stream: 1 feed', function (t) {
  t.plan(6)

  var db = create.one()
  var n = 2

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/foo/bar', 'quux', function (err) {
      t.error(err, 'no error')
      var hs = db.createHistoryStream({live: true})
      hs.on('data', function (node) {
        n--
        if (n === 1) {
          t.equals(node.key, '/a')
          t.equals(node.value, '2')
        } else if (n === 0) {
          t.equals(node.key, '/foo/bar')
          t.equals(node.value, 'quux')
        }
      })
    })
  })
})

tape('live stream: 1 feed', function (t) {
  t.plan(7)

  var db = create.one()

  var hs = db.createHistoryStream({live: true})
  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/foo/bar', 'quux', function (err) {
      t.error(err, 'no error')
      collectN(hs, 2, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual[0].key, '/a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, '/foo/bar')
        t.equals(actual[1].value, 'quux')
      })
    })
  })
})

tape('live stream: 1 feed, start version', function (t) {
  t.plan(9)

  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.version(function (err, version) {
      t.error(err, 'no error')
      var hs = db.createHistoryStream({live: true, start: version})
      db.put('/foo/bar', 'quux', function (err) {
        t.error(err, 'no error')
        db.put('/a', '17', function (err) {
          t.error(err, 'no error')
          collectN(hs, 2, function (err, actual) {
            t.error(err, 'no error')
            t.equals(actual[0].key, '/foo/bar')
            t.equals(actual[0].value, 'quux')
            t.equals(actual[1].key, '/a')
            t.equals(actual[1].value, '17')
          })
        })
      })
    })
  })
})

tape('2 feeds: live', function (t) {
  create.two(function (a, b) {
    var hsA = a.createHistoryStream({live: true})
    var hsB

    a.put('/a', 'a', function (err) {
      t.ifError(err)
      b.put('/a', 'b', function (err) {
        t.ifError(err)
        b.version(function (err, start) {
          t.ifError(err)
          hsB = b.createHistoryStream({live: true, start: start})
          a.put('/a', 'c', function (err) {
            t.ifError(err)
            replicate(a, b, function () {
              validate()
            })
          })
        })
      })
    })

    function validate () {
      collectN(hsA, 4, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 4)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
        t.equals(actual[2].feed + ',' + actual[2].seq, '0,2')
        t.equals(actual[3].feed + ',' + actual[3].seq, '1,0')

        collectN(hsB, 3, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 3)
          t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
          t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
          t.equals(actual[2].feed + ',' + actual[2].seq, '0,2')
          t.end()
        })
      })
    }
  })
})

tape('2 feeds: live + 3rd feed added', function (t) {
  create.two(function (a, b) {
    var hs = a.createHistoryStream({live: true})
    var c = create.one(a.key)

    a.put('/a', 'a', function (err) {
      t.ifError(err)
      b.put('/a', 'b', function (err) {
        t.ifError(err)
        replicate(a, b, function () {
          c.put('/foo/bar', 'bax', function (err) {
            t.ifError(err)
            a.authorize(c.local.key, function (err) {
              t.ifError(err)
              c.put('/a', 'c', function (err) {
                t.ifError(err)
                replicate(a, c, function () {
                  a.put('/foo/bar', 'box', function (err) {
                    t.ifError(err)
                    validate()
                  })
                })
              })
            })
          })
        })
      })
    })

    function validate () {
      collectN(hs, 7, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 7)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,0')
        t.equals(actual[3].feed + ',' + actual[3].seq, '0,2')
        t.equals(actual[4].feed + ',' + actual[4].seq, '2,0')
        t.equals(actual[5].feed + ',' + actual[5].seq, '2,1')
        t.equals(actual[6].feed + ',' + actual[6].seq, '0,3')
        t.end()
      })
    }
  })
})

function collect (stream, cb) {
  var res = []
  stream.on('data', res.push.bind(res))
  stream.once('error', cb)
  stream.once('end', cb.bind(null, null, res))
}

function collectN (stream, expected, cb) {
  var res = []
  stream.on('data', function (node) {
    if (expected <= 0) {
      return stream.emit('error', new Error('extra entries emitted'))
    }
    res.push(node)
    if (!--expected) cb(null, res)
  })
  stream.once('error', cb)
}

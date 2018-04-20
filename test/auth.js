var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var run = require('./helpers/run')

tape('authorized writer passes "authorized" api', function (t) {
  create.two(function (a, b) {
    a.put('foo', 'bar', function (err) {
      t.error(err)
      a.authorized(a.local.key, function (err, auth) {
        t.error(err)
        t.equals(auth, true)
        b.authorized(b.local.key, function (err, auth) {
          t.error(err)
          t.equals(auth, true)
          t.end()
        })
      })
    })
  })
})

tape('authorized writer passes "authorized" api', function (t) {
  create.two(function (a, b) {
    b.put('foo', 'bar', function (err) {
      t.error(err)
      a.authorized(a.local.key, function (err, auth) {
        t.error(err)
        t.equals(auth, true)
        b.authorized(b.local.key, function (err, auth) {
          t.error(err)
          t.equals(auth, true)
          t.end()
        })
      })
    })
  })
})

tape('unauthorized writer fails "authorized" api', function (t) {
  var a = create.one()
  a.ready(function () {
    var b = create.one(a.key)
    b.ready(function () {
      b.authorized(b.local.key, function (err, auth) {
        t.error(err)
        t.equals(auth, false)
        t.end()
      })
    })
  })
})

tape('local unauthorized writes =/> authorized', function (t) {
  var a = create.one()
  a.ready(function () {
    var b = create.one(a.key)
    b.ready(function () {
      b.put('/foo', 'bar', function (err) {
        t.error(err)
        b.authorized(b.local.key, function (err, auth) {
          t.error(err)
          t.equals(auth, false)
          b.authorized(a.local.key, function (err, auth) {
            t.error(err)
            t.equals(auth, true)
            t.end()
          })
        })
      })
    })
  })
})

tape('unauthorized writer doing a put after replication', function (t) {
  t.plan(1)
  var a = create.one()
  a.ready(function () {
    var b = create.one(a.key)
    b.ready(function () {
      replicate(a, b, function () {
        b.put('foo', 'bar', function (err) {
          t.error(err)
        })
      })
    })
  })
})

tape('unauthorized writer fails "authorized" after some writes', function (t) {
  var a = create.one()
  a.ready(function () {
    run(
      cb => a.put('foo', 'bar', cb),
      cb => a.put('foo', 'bar2', cb),
      cb => a.put('foo', 'bar3', cb),
      cb => a.put('foo', 'bar4', cb),
      done
    )

    function done (err) {
      t.error(err)
      var b = create.one(a.key)
      b.ready(function () {
        replicate(a, b, function () {
          b.authorized(b.local.key, function (err, auth) {
            t.error(err)
            t.equals(auth, false)
            t.end()
          })
        })
      })
    }
  })
})

tape('authorized is consistent', function (t) {
  t.plan(5)

  var a = create.one(null, {contentFeed: true})
  a.ready(function () {
    var b = create.one(a.key, {contentFeed: true, latency: 10})

    run(
      cb => b.put('bar', 'foo', cb),
      cb => a.put('foo', 'bar', cb),
      auth,
      replicate.bind(null, a, b),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      a.authorized(b.local.key, function (err, auth) {
        t.error(err, 'no error')
        t.ok(auth)
      })
      b.authorized(b.local.key, function (err, auth) {
        t.error(err, 'no error')
        t.ok(auth)
      })
    }

    function auth (cb) {
      a.authorize(b.local.key, cb)
    }
  })
})

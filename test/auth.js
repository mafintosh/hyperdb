var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

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

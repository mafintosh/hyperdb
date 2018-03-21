var tape = require('tape')
var create = require('./helpers/create')

tape('authorized writer passes "authorized" api', function (t) {
  create.two(function (a, b) {
    b.authorized(b.local.key, function (err, auth) {
      t.error(err)
      t.equals(auth, true)
      t.end()
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

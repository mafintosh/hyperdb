if (typeof window === 'undefined') {
  module.exports = require('./create-node')
} else {
  module.exports = require('./create-browser')
}

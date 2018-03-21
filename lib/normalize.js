module.exports = function normalizeKey (key) {
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}

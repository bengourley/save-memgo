require('should')

var memgo = require('../')
  , collection

function getEngine(callback) {
  var collection = memgo('test-' + Math.random() * 100)
  callback(undefined, collection)
}

require('save/test/engine.tests')('_id', getEngine)
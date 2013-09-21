var memgo = require('../')
function getEngine(callback) {
  var collection = memgo('test-' + Math.random() * 100)
  callback(undefined, collection)
}

require('save/test/engine.tests')('_id', getEngine)

describe('save-memgo', function () {
  it('should allow $in operator in find query', function (done) {
    getEngine(function (errr, collection) {
      collection.create({ a : 1 }, function (error, data) {
        collection.find({ a: { $in: [1] }}, function (error, results) {
          results[0]._id = data._id
          done()
        })
      })
    })
  })
})
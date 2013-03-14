module.exports = createCollection

var scule = require('sculejs')
  , extend = require('util')._extend
  , emptyFn = function () {}
  , EventEmitter = require('events').EventEmitter

function createCollection(name, options) {

  var collection = scule.db.factoryCollection('scule+dummy://' + name)
    , defaults = { idProperty: '_id' }
    , self = new EventEmitter()

  options = options || {}
  extend(options, defaults)

  function create(object, callback) {
    callback = callback || emptyFn
    // if id is any falsy consider it empty
    if (!object[options.idProperty]) {
      delete object[options.idProperty]
    }
    self.emit('create', object)
    var a = extend({}, object)

    //@todo
    // save-mongodb uses .insert()
    // but scule only has .save()
    collection.save(a, function (objectId) {

      // save will never error with scule
      // so the callback just receives the
      // id of the object that was save

      a[options.idProperty] = objectId
      objectIdToString(a)
      self.emit('afterCreate', a)
      callback(null, a)

    })
  }

  function createOrUpdate(object, callback) {
    if (typeof object[options.idProperty] === 'undefined') {
      // Create a new object
      self.create(object, callback)
    } else {
      // Try and find the object first to update

      self.read(object[options.idProperty], function(err, entity) {
        if (err) {
          return callback(err)
        }
        if (entity) {
          // We found the object so update
          self.update(object, callback)
        } else {
          // We didn't find the object so create
          self.create(object, callback)
        }
      })
    }
  }

  function read(id, callback) {
    var oId
      , query = {}

    self.emit('read', id)

    try {
      oId = scule.db.getObjectId(id)
    } catch (e) {
      if (e.message === 'Argument passed in must be a single String of 12 bytes or a string of 24 hex characters') {
        return callback(undefined, undefined)
      }
    }

    callback = callback || emptyFn
    collection.findOne(oId, function (entity) {
      callback(null, entity === null ? undefined : objectIdToString(entity))
    })
  }

  function update(object, overwrite, callback) {
    if (typeof overwrite === 'function') {
      callback = overwrite
      overwrite = false
    }

    self.emit('update', object, overwrite)
    callback = callback || emptyFn
    var query = {}
      , updateObject = extend({}, object)
      , updateData = { $set: updateObject }
      , id = object[options.idProperty]

    if (overwrite) {
      updateData.$unset = {}
      var original = collection.findOne(object[options.idProperty])
      Object.keys(original).forEach(function (key) {
        if (typeof updateObject[key] === 'undefined') updateData.$unset[key] = true
      })
    }

    if (id === undefined || id === null) {
      return callback(new Error('Object has no \''
        + options.idProperty + '\' property'))
    }


    query[options.idProperty] = id;
    delete updateObject[options.idProperty]
    collection.update(query, updateData, {}, true, function (entity) {

      if (!entity.length) {
        callback(new Error('No object found with \'' + options.idProperty +
          '\' = \'' + id + '\''))
      } else {
        // entity = objectIdToString(entity)
        self.emit('afterUpdate', entity[0])
        callback(null, entity[0])
      }
    })
  }

  function deleteMany(query, callback) {
    self.emit('deleteMany', query)
    callback = callback || emptyFn
    collection.remove(query, {}, function () {
      self.emit('afterDeleteMany', query)
      callback(null)
    })
  }

   /**
   * Deletes one object. Returns an error if the object can not be found
   * or if the ID property is not present.
   *
   * @param {Object} object to delete
   * @param {Function} callback
   * @api public
   */
  function del(id, callback) {

    callback = callback || emptyFn

    if (typeof callback !== 'function') {
      throw new TypeError('callback must be a function or empty')
    }

    self.emit('delete', id)
    var query = {}
    query[options.idProperty] = scule.db.getObjectId(id)
    try {
      collection.remove(query, {}, function () {
        self.emit('afterDelete', id)
        callback()
      })
    } catch (e) {
      // Query didn't match any objects
      callback(e)
    }
  }

  // Because your application using save shouldn't know about engine internals
  // ObjectID must be converted to strings before returning.
  function objectIdToString(entity) {
    entity[options.idProperty] = entity[options.idProperty].toString()
    return entity
  }

  function normaliseSortOptions(options) {
    var sort = options.sort
    options.$sort = {}
    if (Array.isArray(options.sort)) {
      options.sort.forEach(function (prop) {
        options.$sort[prop[0]] = (prop[1] === 'desc' || prop[1] === -1) ? -1 : 1
      })
    } else if (typeof options.sort === 'object') {
      Object.keys(options.sort).forEach(function (key) {
        options.$sort[key] = (options.sort[key] === 'desc' || options.sort[key] === -1) ? -1 : 1
      })
    } else if (typeof options.sort === 'string') {
      options.$sort[options.sort] = 1
    }
    delete options.sort
  }

  function find(query, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    if (typeof callback !== 'function') {
      throw new Error('callback must be a function')
    }
    normaliseSortOptions(options)
    self.emit('find', query)
    var found = collection.find(query, options)
    if (found.length === 1 && found[0] === null) return callback(null, [])
    callback(undefined, found)//data.map(objectIdToString))
  }

  function findOne(query, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    extend(options, { limit: 1 })
    normaliseSortOptions(options)
    self.emit('findOne', query)
    var found = collection.find(query, options)
    if (found.length === 1 && found[0] === null) return callback(null, undefined)
    if (found.length) return callback(null, found[0])
    return callback(null, undefined)
  }

  function count(query, callback) {
    self.emit('count', query)
    collection.count(query, {}, function (count) {
      callback(null, count)
    })
  }

  extend(self,
    { create: create
    , createOrUpdate: createOrUpdate
    , read: read
    , update: update
    , deleteMany: deleteMany
    , 'delete': del
    , find: find
    , findOne: findOne
    , count: count
    , idProperty: options.idProperty
    , idType: String
    , _collection: collection
    })

  return self
}
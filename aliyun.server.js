// We use the official aws sdk
OSS = Npm.require('aliyun-sdk').OSS;

function pick(obj, keys) {
  var result = {}, iteratee = keys[0];
  if (obj == null || arguments.length < 2) return result;
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (obj.hasOwnProperty(key)) {
      result[key] = obj[key];
    }
  }
  return result;
}

/**
 * Creates an Aliyun OSS store instance on server. Inherits `FS.StorageAdapter`
 * type.
 *
 * @public
 * @constructor
 * @param {String} name      The store name
 * @param {Object} options   Storage options
 * @return {FS.Store.OSS}    An instance of FS.StorageAdapter.
 */
FS.Store.OSS = function(name, options) {
  var self = this;
  if (!(self instanceof FS.Store.OSS))
    throw new Error('FS.Store.OSS missing keyword "new"');

  options = options || {};

  // Determine which folder (key prefix) in the bucket to use
  var folder = options.folder;
  folder = typeof folder === 'string' && folder.length ?
           folder.replace(/^\//, '').replace(/\/?$/, '/') : '';

  // Determine which bucket to use, reruired
  if (!options.hasOwnProperty('bucket')) {
    throw new Error('FS.Store.OSS requires "buckect"');
  }

  // var defaultAcl = options.ACL || 'private';

  var serviceParams = FS.Utility.extend({
    accessKeyId: null, // Required
    accessKeySecret: null, // Required
    bucket: null, // Required
    region: 'oss-cn-hangzhou',
    internal: false,
    timeout: 60000
  }, options);
  // Create S3 service
  var ossStore = new OSS.createClient(serviceParams);

  return new FS.StorageAdapter(name, options, {
    typeName: 'storage.oss',
    fileKey: function(fileObj) {
      // Lookup the copy
      var info = fileObj && fileObj._getInfo(name);
      // If the store and key is found return the key
      if (info && info.key) return info.key;

      var filename = fileObj.name();
      var filenameInStore = fileObj.name({store: name});

      // If no store key found we resolve / generate a key
      return fileObj.collectionName + '/' +
             fileObj._id + '-' + (filenameInStore || filename);
    },

    // Bucket, Key,
    createReadStream: function(fileKey, options) {
      var readOptions = pick(options, ['timeout', 'headers']);
      return ossStore.getStream(fileKey, readOptions);
    },
    // Comment to documentation: Set options.ContentLength otherwise the
    // indirect stream will be used creating extra overhead on the filesystem.
    // An easy way if the data is not transformed is to set the
    // options.ContentLength = fileObj.size ...
    createWriteStream: function(fileKey, options) {
      options = options || {};

      // We dont support array of aliases
      delete options.aliases;
      // We dont support contentType
      delete options.contentType;
      // We dont support metadata use Metadata?
      delete options.metadata;

      // Set options
      var options = FS.Utility.extend({
        Bucket: bucket,
        Key: folder + fileKey,
        fileKey: fileKey,
        ACL: defaultAcl
      }, options);

      return ossStore.createWriteStream(options);
    },
    remove: function(fileKey, callback) {

      S3.deleteObject({
        Bucket: bucket,
        Key: folder + fileKey
      }, function(error) {
        callback(error, !error);
      });
    },
    watch: function() {
      throw new Error('OSS does not support watch.');
    }
  });
};

var stream = Npm.require('stream');

/**
 * Wraps official put stream
 * @param  {[type]} params [description]
 * @param  {[type]} option [description]
 * @return {[type]}        [description]
 */
OSS.prototype.createReadStream = function(params, option) {
  var streams = AWS.util.nodeRequire('stream');
  var req = this;
  var stream = null;

  if (AWS.HttpClient.streamsApiVersion === 2) {
    stream = new streams.PassThrough();
    req.send();
  } else {
    stream = new streams.Stream();
    stream.readable = true;

    stream.sent = false;
    stream.on('newListener', function(event) {
      if (!stream.sent && event === 'data') {
        stream.sent = true;
        process.nextTick(function() { req.send(); });
      }
    });
  }

  this.on('httpHeaders', function streamHeaders(statusCode, headers, resp) {
    if (statusCode < 300) {
      req.removeListener('httpData', AWS.EventListeners.Core.HTTP_DATA);
      req.removeListener('httpError', AWS.EventListeners.Core.HTTP_ERROR);
      req.on('httpError', function streamHttpError(error) {
        resp.error = error;
        resp.error.retryable = false;
      });

      var httpStream = resp.httpResponse.createUnbufferedStream();
      if (AWS.HttpClient.streamsApiVersion === 2) {
        httpStream.pipe(stream);
      } else {
        httpStream.on('data', function(arg) {
          stream.emit('data', arg);
        });
        httpStream.on('end', function() {
          stream.emit('end');
        });
      }

      httpStream.on('error', function(err) {
        stream.emit('error', err);
      });
    }
  });

  this.on('error', function(err) {
    stream.emit('error', err);
  });

  return stream;

};

//"ACL":'lcation": "header","name": "x-oss-acl"}
//"Bucket": {"required": true,"location": "uri"}
//"CacheControl": {"location": "header","name": "Cache-Control"}
//"ContentDisposition": {"location": "header","name": "Content-Disposition"}
//"ContentEncoding": {"location": "header","name": "Content-Encoding"}
//"ContentLanguage": {"location": "header","name": "Content-Language"}
//"ContentType": {"location": "header","name": "Content-Type"}
//"Expires": {"type": "timestamp","location": "header","name": "Expires"}
//"GrantFullControl": {"location": "header","name": "x-oss-grant-full-control"}
//"GrantRead": {"location": "header","name": "x-oss-grant-read"}
//"GrantReadACP": {"location": "header","name": "x-oss-grant-read-acp"}
//"GrantWriteACP": {"location": "header","name": "x-oss-grant-write-acp"}
//"Key": {"required": true,"location": "uri"}
//"Metadata": {"type": "map","location": "header","name": "x-oss-meta-","members": {},"keys": {}}
//"ServerSideEncryption": {"location": "header","name": "x-oss-server-side-encryption"}
//"StorageClass": {"location": "header","name": "x-oss-storage-class"}
//"WebsiteRedirectLocation": {"location": "header","name": "x-oss-website-redirect-location"}


/**
 * Creates get put stream, inspired by github.com/meteormatt:
 * https://github.com/meteormatt/oss-upload-stream
 * @param  {Object} params CollectionFS Params
 * @param  {Object} option CollectionFS Options
 * @return {Stream}        writeStream object
 */
OSS.prototype.createPutStream = function(params, option) {
  var self = this;

  // Scope variables
  // Create the writable stream interface.
  var writeStream = new stream.Writable({highWaterMark: 4194304}); // 4MB
  var multipartUploadID = null;
  var chunkSizeThreashold = 5242880;
  var awaitingCallback;
  var fileKey = params && (params.fileKey || params.Key);

  // Current chunk
  var currentChunk = Buffer(0);
  var chunkNumber = 1;

  // Status
  var parts = [];
  var receivedSize = 0;
  var uploadedSize = 0;

  var runWhenReady = function(callback) {
    // If we dont have a upload id we are not ready
    if (multipartUploadID === null) {
      // We set the waiting callback
      awaitingCallback = callback;
    } else {
      // No problem - just continue
      callback();
    }
  };

  // Handler to receive data and upload it to OSS.
  writeStream._write = function(incomingChunk, enc, next) {
    currentChunk = Buffer.concat([currentChunk, incomingChunk]);

    // While the current chunk is larger than chunkSizeThreashold, we flush
    // the chunk buffer to OSS via multipart upload.
    if (curentChunk.length > chunkSizeThreashold) {
      // Upload when necessary;
      runWhenReady(function() { flushChunk(next, false); });
    } else {
      runWhenReady(next);
    }
  };

  // Hijack the end method, send to OSS and complete.
  var _originalEnd = writeStream.end;
  writeStream.end = function(chunk, encoding, callback) {
    _originalEnd.call(this, chunk, encoding, function() {
      runWhenReady(function() { flushChunk(callback, true); });
    });
  };

  /**
   * Flushes chunk to Aliyun
   * @param  {Function} callback  Callback, normally for next part of data.
   * @param  {Boolean}  lastChunk If it's the last chunk.
   * @return {undefined}
   */
  function flushChunk(callback, lastChunk) {
    if (multipartUploadID === null) {
      throw new Error('OSS Client Error: Missing mulitipart upload ID');
    }

    // Chunk to upload
    var uploadingChunk = Buffer(currentChunk.length);
    currentChunk.copy(uploadingChunk); // copies to target

    var localChunkNumber = chunkNumber++;

    self.uploadPart({
      Body: uploadingChunk,
      Bucket: params.Bucket,
      Key: params.Key,
      UploadId: multipartUploadID,
      PartNumber: localChunkNumber
    }, uploadPartCallback);

    // Reset the current buffer
    currentChunk = Buffer(0);

    function uploadPartCallback(error, result) {
      // Handle error as the top priority;
      if (error) {
        abortUpload('OSS Client Error: ' + JSON.stringify(error));
        return;
      }

      // Next part of data.
      if (typeof callback === 'function') {
        callback();
      }

      uploadedSize += uploadingChunk.length;
      parts[localChunkNumber - 1] = {
        ETag: result.ETag,
        PartNumber: localChunkNumber
      };

      // Debug only.
      // writeStream.emit('chunk', {
      //   ETag: result.ETag,
      //   PartNumber: localChunkNumber,
      //   receivedSize: receivedSize,
      //   uploadedSize: uploadedSize
      // });

      // While incoming stream is finished and we have uploaded everything,
      // we would further notice OSS
      if (writeStream._writableState.ended === true &&
          uploadedSize === receivedSize && lastChunk) {
        closeUploadStream();
      }
    }
  };

  /**
   * Shuts down upload stream, calls Aliyun to merge every chunk of file
   * @return {undefined}
   */
  function closeUploadStream() {
    // Not possible without multipart upload id
    if (!multipartUploadID) {
      return;
    }

    self.completeMultipartUpload({
      Bucket: params.Bucket,
      Key: params.Key,
      UploadId: multipartUploadID,
      CompleteMultipartUpload: {Parts: parts}
    }, function(error, result) {
      if (error) {
        abortUpload('OSS Client Error at Comletion: ' + JSON.stringify(error));
        return;
      }

      if (FS.debug) {
        console.log('SA OSS - DONE!!');
      }
      writeStream.emit('stored', {
        fileKey: fileKey,
        size: uploadedSize,
        storedAt: new Date()
      });
    });
  }

  /**
   * When a fatal error occurs abort the multipart upload
   * @param  {String} errorText Error text
   * @return {undefined}
   */
  function abortUpload(errorText) {
    self.abortMultipartUpload({
      Bucket: params.Bucket,
      Key: params.Key,
      UploadId: multipartUploadID
    }, function(abortError) {
      if (abortError) {
        writeStream.emit('error',
                         errorText + '\nOSS Client Abort Error: ' + abortError);
      } else {
        writeStream.emit('error', errorText);
      }
    });
  };

  self.createMultipartUpload(params, function(error, data) {
    if (error) {
      writeStream.emit('error', 'OSS Client Error: ' + JSON.stringify(error));
      return;
    }
    multipartUploadID = data.UploadId;
    // Call awaiting callback to start upload
    if (typeof awaitingCallback === 'function') {
      awaitingCallback();
    }
  });

  return writeStream;
};


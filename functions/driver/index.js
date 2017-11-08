var crypto = require('crypto');
var AWS = require('aws-sdk');
var lambda = new AWS.Lambda()
var s3 = new AWS.S3()

console.log('starting driver function')

const invoke = (arn, ctx, payload) => {
  var params = {
    FunctionName: arn,
    ClientContext: ctx,
    InvocationType: "RequestResponse",
    Payload: payload
  };
  return new Promise((resolve, reject) => {
    lambda.invoke(params, function(err, data) {
      if(err) reject(err)
      else resolve(data)
    });
  })

}

const mapperOutputPrefix = (config) => {
  return `${config.outputPrefix}/${config.jobId}/mapper/`
}

const reducerOutputPrefix = (config) => {
  return `${config.outputPrefix}/${config.jobId}/reducer/`
}

const listObjects = (bucket, prefix, continuationToken=null, delimiter = "/") => {
  var params = {
    Bucket: bucket,
    Prefix: prefix
  };
  if(delimiter) {
    params.Delimiter = delimiter
  }
  if(continuationToken) {
    params.ContinuationToken = continuationToken
  }
  return new Promise((resolve, reject) => {
    s3.listObjectsV2(params, function(err, data) {
      if (err) reject(err);
      else     resolve(data);
    });
  })
}

const fetchPartitionItems = (outputBucket, prefix, continuationToken=null, resultProcessor, delimiter="/") => {
  var items = []
  return new Promise((resolve, reject) => {
    listObjects(outputBucket, prefix, continuationToken, delimiter)
      .then((data) => {
        console.log("data", data)
        items = resultProcessor(data)
        if(!!data.ContinuationToken) {
          fetchPartitionItems(outputBucket, prefix, data.ContinuationToken, resultProcessor, delimiter)
            .then((moreItems) => {
              resolve([...items, ...moreItems])
            })
        } else {
          resolve(items)
        }
      })
      .catch((err) => {
        console.log("err", err)
        reject(err)
      })
  })
}

const fetchPartitionPrefixes = (outputBucket, prefix, continuationToken=null, delimiter="/") => {
  return fetchPartitionItems(outputBucket, prefix, continuationToken=null, (data) => {
    return data.CommonPrefixes.map((prefix) => {
      return prefix.Prefix
    })
  }, delimiter)
}

const fetchPartitionKeys = (outputBucket, prefix, continuationToken=null, delimiter="/") => {
  return fetchPartitionItems(outputBucket, prefix, continuationToken=null, (data) => {
    return partitionKeys = data.Contents.map((content) => {
      return content.Key
    })
  }, delimiter)
}

const fetchObject = (bucket, key) => {
  var params = {
    Bucket: bucket,
    Key: key
   };
   return new Promise((resolve, reject) => {
     s3.getObject(params, function(err, data) {
       if (err) {
         reject(err)
       }
       else {
         resolve(data)
       }
     })
   })
}

const writePart = (bucket, outputPrefix, jobId, buffer, partId=1) => {
  // write to s3://bucket/outputPrefix/jobId/part-N
  console.log("writing part", jobId, partId)
  var params = {
    Body: buffer,
    Bucket: bucket,
    Key: `${outputPrefix}/${jobId}/part-${partId}`
  };
  return new Promise((resolve, reject) => {
    s3.putObject(params, function(err, data) {
      if (err) {
        console.log("error emitting", err, err.stack); // an error occurred
        reject(err)
      }
      else {
        console.log("success emitting", data);           // successful response
        resolve(data)
      }
    });
  })

}

exports.handle = function(e, ctx, cb) {
  console.log('driver event: %j', e)
  var inputBucket = e.inputBucket
  var inputPaths = e.inputPaths

  var outputBucket = e.outputBucket
  var outputPrefix = e.outputPrefix

  var jobName = e.jobName
  var mapperArn = e.mapperArn
  var reducerArn = e.reducerArn

  var jobRand = crypto.randomBytes(16).toString("hex")
  var jobTime = new Date().getTime()
  var jobId = `${jobTime}-${jobRand}`

  var config = {
    inputBucket,
    outputBucket,
    outputPrefix,
    jobId,
    jobName,
    mapperArn,
    reducerArn
  }
  var clientCtx = new Buffer(JSON.stringify(config)).toString('base64')

  // // start mapcoo function
  // // with given jobid
  // // await completion
  // // TODO in parallel
  var invocations = inputPaths.map((path) => {
    console.log("processing path:", path, ctx)
    var mapperPayload = JSON.stringify({key: path, value: path})
    return invoke(mapperArn, clientCtx, mapperPayload)
  })


  // read S3 output prefix for reducer partitions
  // start reducer function
  // with given jobid
  // await completion
  Promise.all(invocations).then(() => {
    // get partitionKeys
    fetchPartitionPrefixes(outputBucket, mapperOutputPrefix(config))
    .then((partitionKeys) => {
      console.log("partitionKeys", partitionKeys)
      // get paths for each PartitionKey
      var partitionKeyPathMapper = partitionKeys.map((partitionKey) => {
        return fetchPartitionKeys(outputBucket, partitionKey)
        .then((partitionKeyPaths) => {
          return {key: partitionKey, paths: partitionKeyPaths}
        })
        .catch((err) => {
          console.log("error", err)
          return null
        })
      })
      Promise.all(partitionKeyPathMapper).then((partitionKeyPathMap) => {
        console.log("partitionKeyPathMap", partitionKeyPathMap)

        // invoke reducer per partition key
        var reducerInvocations = partitionKeyPathMap.map((partitionKeyAndPath) => {
          console.log("processing partitionKey:", partitionKeyAndPath)
          const {key, paths} = partitionKeyAndPath
          var reducerPayload = JSON.stringify({key, value: paths})
          return invoke(reducerArn, clientCtx, reducerPayload, function(err, data) {
            if (err) {
              console.log("error!!!")
              console.log(err, err.stack); // an error occurred
            }
            else {
              console.log("reducer data!!!")
              console.log(data);           // successful response
              return data
            }
          })
        })
        Promise.all(reducerInvocations)
        .then(() => {
          // list reducer outputs - calc total size of all output
          // concat up to 5MB per part
          // if over 5MB for single part, use multipart upload
          // if over 64MB total split into multiple multipart uploads
          // get reducer output
          fetchPartitionKeys(outputBucket, reducerOutputPrefix(config), null, null)
          .then((reducerKeys) => {
            console.log("reducerKeys", reducerKeys)
            var totalLength = 0
            var buffers = []
            var items = reducerKeys.map((reducerKey) => {
              return fetchObject(outputBucket, reducerKey)
            })
            Promise.all(items)
            .then((items) => {
              console.log("Items", items)
              items.forEach((item) => {
                totalLength += item.ContentLength
                buffers.push(item.Body)
              })
              console.log("totalLength", totalLength)
              var allBuffer = Buffer.concat(buffers, totalLength)
              writePart(outputBucket, outputPrefix, jobId, allBuffer)
              .then(() => {
                cb(null, { data: allBuffer.toString(), status: 'done' })
              })
              .catch((err) => {
                cb(err)
              })
            })

          })
          .catch((err) => {
            cb(err)
          })


        })
        .catch((err) => {
          cb(err)
        })
      })
    })
  })

}

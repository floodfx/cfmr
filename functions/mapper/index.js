var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3()

console.log('starting mapper function')

const emit = (key, value, ctx) => {
  // write to s3://outputPrefix/{key}/{uuid}
  console.log(key, value)
  var hashedKey = crypto.createHash('sha256').update(key).digest("hex")
  var objectKey = crypto.randomBytes(16).toString("hex")
  var params = {
    Body: JSON.stringify({key, value}),
    Bucket: ctx.clientContext.outputBucket,
    Key: `${ctx.clientContext.outputPrefix}/${ctx.clientContext.jobId}/mapper/${hashedKey}/${objectKey}`
  };
  s3.putObject(params, function(err, data) {
     if (err) {
       console.log("error emitting", err, err.stack); // an error occurred
     }
     else {
       console.log("success emitting", data);           // successful response
     }
  });
}

exports.handle = function(e, ctx, cb) {
  console.log('map event: %j', e, ctx)

  var params = {
    Bucket: ctx.clientContext.inputBucket,
    Key: e.value
   };
   s3.getObject(params, function(err, data) {
     if (err) {
       console.log(err, err.stack); // an error occurred
       cb(err)
     }
     else {
       console.log(data);           // successful response
       try {
         var words = data.Body.toString().split(/\s+/).forEach((word) => {
           emit(word, 1, ctx)
         })
         cb(null, words)
       } catch (e) {
         cb(e)
       }
     }

   });
}

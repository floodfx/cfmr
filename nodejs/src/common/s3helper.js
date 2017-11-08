var AWS = require('aws-sdk');
var s3 = new AWS.S3()

const getItem = (bucket, key) => {
  var params = {
    Bucket: bucket,
    Key: key
   };
   return new Promise((resolve, reject) => {
     s3.getObject(params, function(err, data) {
       if (err) {
         console.log(err, err.stack); // an error occurred
         reject(err)
       }
       else {
         console.log(data);           // successful response
         resolve(data)
       }
     });
   })
}

module.exports = {
  getItem
}

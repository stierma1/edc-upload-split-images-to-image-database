"use strict"

var Worker = require("basic-distributed-computation").Worker;
var uuid = require("uuid");
var MongoClient = require("mongodb").MongoClient;

class Upload extends Worker {
  constructor(parent){
    super("upload-split-images-to-image-database", parent);
    this.db = null;
    this.init = new Promise((resolve, reject) => {
      MongoClient.connect(process.env.IMAGE_DATABASE_URL, (err, db) => {
        if(err){
          reject(err);
          return;
        }
        this.db = db;
        resolve();
      });
    });
  }

  work(req, inputKey, outputKey){
    this.init.then(() => {
      var inputVal = req.body;
      if(inputKey){
        inputVal = req.body[inputKey];
      }
      var collectionName = null;
      var splitImageSearchs = inputVal.splitFrames;
      var splitImagesProms = splitImageSearchs.map((search) => {
        return this.getSessionData(req, search);
      });
      Promise.all(splitImagesProms)
        .then((sessionDatas) => {
          return sessionData.map((sessionData) => {
            return sessionData.data;
          });
        })
        .then((imageDatas) => {
          var collectionName = inputVal.imageId || uuid.v4();
          var collectionCursor = this.db.collection(collectionName);
          collectionCursor.insertMany(imageDatas, (err, results) => {
            if(err){
              req.status(err).next();
              return;
            }
            if(outputKey){
              req.body[outputKey] = collectionName;
            } else {
              req.body = collectionName;
            }
            req.next();
          });
        })
        .catch((err) => {
          req.status(err).next();
        });
    }).catch((err) => {
      req.status(err).next();
    });
  }
}

module.exports = Upload;

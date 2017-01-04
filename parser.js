"use latest";

const mongojs = require('mongojs'),
  _ = require('underscore'),
  storage = require('google-cloud').storage,
  json2csv = require('json2csv'),
  fs = require('fs');

const collections = ['activities'],
      course = process.argv.slice(2)[0],
      RATE_LIMIT = 20; //max requests per second

/* Extend the Underscore object with the following methods */

// https://gist.github.com/mattheworiordan/1084831

// Rate limit ensures a function is never called more than every [rate]ms
// Unlike underscore's _.throttle function, function calls are queued so that
//   requests are never lost and simply deferred until some other time
//
// Parameters
// * func - function to rate limit
// * rate - minimum time to wait between function calls
// * async - if async is true, we won't wait (rate) for the function to complete before queueing the next request
//
// Example
// function showStatus(i) {
//   console.log(i);
// }
// var showStatusRateLimited = _.rateLimit(showStatus, 200);
// for (var i = 0; i < 10; i++) {
//   showStatusRateLimited(i);
// }
//
// Dependencies
// * underscore.js
//
_.rateLimit = function(func, rate, async) {
  var queue = [];
  var timeOutRef = false;
  var currentlyEmptyingQueue = false;
  
  var emptyQueue = function() {
    if (queue.length) {
      currentlyEmptyingQueue = true;
      _.delay(function() {
        if (async) {
          _.defer(function() { queue.shift().call(); });
        } else {
          queue.shift().call();
        }
        emptyQueue();
      }, rate);
    } else {
      currentlyEmptyingQueue = false;
    }
  };
  
  return function() {
    var args = _.map(arguments, function(e) { return e; }); // get arguments into an array
    queue.push( _.bind.apply(this, [func, this].concat(args)) ); // call apply so that we can pass in arguments as parameters as opposed to an array
    if (!currentlyEmptyingQueue) { emptyQueue(); }
  };
};

/**
 * Connects to MongoDb
 *
 * @param {String} mongo connection string
 * @returns {Object} Returns connection promise
 */
const connectToDb = mongoUrl => {
  return new Promise((res, rej) => {
    try {
      const db = mongojs(mongoUrl, collections, { ssl: true });
      res(db);
    } catch (err) {
      rej(err);
    }
  });
};

/**
 * Constructs file names for GCloud
 *
 * @param {String} file name from db
 * @returns {String} Returns file name with extension
 */
const getFileName = file => {
  file = file.replace('・', '').replace('?', '_').replace('/', '_');
  const fileName = `${file}.mp3`;
  return fileName;
};

/**
 * Initializes GCloud connection
 *
 * @param {String} projectId 
 * @param {String} keyFilename
 * @returns {Object} Returns gcloud storage instance
 */
const initGCloud = (projectId, keyFilename) => {
  return storage({ projectId, keyFilename });
};

/**
 * Checks file existence in Gcloud
 *
 * @param {Object} llBucket - GCloud bucket 
 * @param {String} word - file/word to be looked up
 * @returns {Object} Returns promise indicating file existence
 */
const checkGCloud = (llBucket, word) => {
  const fileName = getFileName(word);
  const dir = 'audio/' + course;

  const firstDirFile = llBucket.file(`${dir}/${fileName}`);

  return firstDirFile.exists()
    .then(res => {
      if (res && res.length > 0 && !res[0]) {
        // word not found
        return { word, found: false };
      } else return { word, found: true };
    })
    .catch(err => {
      // @Todo: Handle Gcloud connection errors
      console.warn(`An error occurred while checking existence of ${fileName}: ${err}`);
    })
}

/**
 * Filters a promise array for files not found in gcloud
 *
 * @param {Object} promise - A promise of data array
 * @param {String} collection - mongo collection being processed
 * @returns {Object} Returns null
 */
const filterAndWrite = (promise, collection) => {
  Promise.all(promise)
    .then(resp => {
      const newArr = _.flatten(resp);
      const notFound = _.where(newArr, { found: false });
      writeToCSV(notFound, collection);
    })
    .catch(err => {
      console.log(`error, ${err}`)
    })
}

/**
 * Writes data to csv
 *
 * @param {Object} data - data array
 * @param {String} collection - mongo collection being processed
 * @returns {Object} Returns null
 */
const writeToCSV = (data, collection) => {
  const fields = ['word'];

  try {
    const csv = json2csv({ data, fields });
    fs.writeFile(`${collection}-${course}.csv`, csv, function(err) {
      if (err) throw err;
      console.log(`words not found in ${collection} collection saved \n ------- \n`);
    });
  } catch (err) {
    console.error(err);
  }
}

/**
 * Trasverses through vocab collection cursors - Does main processing for vocab collection
 *
 * @param {Object} db - mongo database instance
 * @param {Object} llBucket - mGCloud bucket 
 * @returns {Object} Returns a Promise
 */
const processVocab = (db, llBucket) => {
  console.info('Processing vocab collection...\n');
  
  const checkGCloudThrottled = _.rateLimit(checkGCloud, 500);

  return new Promise((res, rej) => {
    db.vocab.find({ word: { $ne: null }, course: course }, { word: 1, _id: 0 })
      .map(vocabRecord => {
        if (vocabRecord && vocabRecord.word) {
          const word = vocabRecord.word;
          return checkGCloudThrottled(llBucket, word);
        }
      }, (err, result) => {
        if (err) {
          rej(err);
        } else {          
          filterAndWrite(result, 'vocab');
          res('writing vocab to file...')
        }
      })
  })
}

/**
 * Trasverses through activities collection cursors - Does main processing for activities collection
 *
 * @param {Object} db - mongo database instance
 * @param {Object} llBucket - mGCloud bucket 
 * @returns {Object} Returns a Promise
 */
const processActivities = (db, llBucket) => {
  return new Promise((res, rej) => {
    console.info('Processing activities collection...\n');

    const checkGCloudThrottled = _.rateLimit(checkGCloud, 1000/RATE_LIMIT+5);

    db.activities.aggregate([{
      $match: {
        'course': course,
        'content.type': 'example-sentence'
      }
    }, {
      $project: {
        _id: 0,
        content: {
          $filter: {
            input: '$content',
            as: 'cont',
            cond: {
              $eq: ['$$cont.type', 'example-sentence']
            }
          }
        }
      }
    }]).map(result => {
      if (result) {
        const promises = _.map(result.content, content => {
          if (content && content.content && content.content.audio) {
            const audio = content.content.audio;
            return checkGCloudThrottled(llBucket, audio);
          }
        });
        return Promise.all(promises);
      }
    }, (err, result) => {
      if (err) {
        rej(err);
      } else {
        filterAndWrite(result, 'activities');
        res('writing activities to file...');
      }
    });
  })
}

/**
 * Entry point to script
 *
 * @param {Object} ctx - context object on webtask.io
 * @param {Object} cb - callback fn on webtask.io 
 * @returns {Object} Returns null
 */
const parser = (ctx, cb) => {
  const MONGO_URL = ctx && ctx.data ? ctx.data.MONGO_URL : undefined || process.env.MONGO_URL || 'mongodb://meteor:PASSWORD@aws-us-east-1-portal.14.dblayer.com:10166/ll-app';
  const GC_PROJECT_ID = ctx && ctx.data ? ctx.data.GC_PROJECT_ID : undefined || process.env.GC_PROJECT_ID || 'project_id';
  const GC_KEY_PATH = ctx && ctx.data ? ctx.data.GC_KEY_PATH : undefined || process.env.GC_KEY_PATH || 'LinguaLift-14b1255b3d5d.json';

  // if (!MONGO_URL) return cb(new Error('MONGO_URL secret is missing'));
  const gcs = initGCloud(GC_PROJECT_ID, GC_KEY_PATH);
  const llBucket = gcs.bucket('ll-app');
  let mongodb;

  connectToDb(MONGO_URL)
    .then(db => {
      mongodb = db
      return processActivities(db, llBucket);
    })
    /*.then(resp => {
      console.info(`${resp}\n`);
      return processVocab(mongodb, llBucket);
    })*/
    .then(resp => {
      console.info(`${resp}\n`);
      mongodb.close();
    })
    .catch(err => {
      console.error(err);
    })
}

parser();
// export default parser;

"use latest";

const mongojs = require('mongojs'),
  _ = require('underscore'),
  storage = require('google-cloud').storage,
  json2csv = require('json2csv'),
  fs = require('fs');

const collections = ['vocab', 'activities'];

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
  file = file.replace(/\./g, '');
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
  const firstSubDir = 'audio/jp';
  const secondSubDir = 'audio/ru';

  const firstDirFile = llBucket.file(`${firstSubDir}/${fileName}`);

  return firstDirFile.exists()
    .then(res => {
      if (res && res.length > 0)
        return res[0];
      else return false;
    })
    .then(isFound => {
      if (!isFound) {
        // check the second directory for file
        const secondDirFile = llBucket.file(`${secondSubDir}/${fileName}`);
        return secondDirFile.exists();
      } else return isFound;
    })
    .then(res => {
      if (res && res.length > 0 && !res[0]) {
        // word not found in both directories
        return { word, found: false };
      } else return { word, found: true };
    })
    .catch(err => {
      console.warn(`An error occurred while checking file existence: ${err}`);
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
    fs.writeFile(`${collection}.csv`, csv, function(err) {
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

  const limit = 2000; // limit number of files due to slow local environment
  return new Promise((res, rej) => {

    db.vocab.find({ word: { $ne: null } }, { word: 1, _id: 0 })
      .limit(limit)
      .map(vocabRecord => {
        if (vocabRecord && vocabRecord.word) {
          const word = vocabRecord.word;
          return checkGCloud(llBucket, word);
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
  console.info('Processing activities collection...\n');

  const limit = 200; // limit number of files due to slow local environment
  return new Promise((res, rej) => {
    db.activities.aggregate([{
      $match: {
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
    }, {
      $limit: limit
    }]).map(result => {
      if (result) {
        const promises = _.map(result.content, content => {
          if (content && content.content && content.content.audio) {
            const audio = content.content.audio;
            return checkGCloud(llBucket, audio)
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
      return processVocab(db, llBucket);
    })
    .then(resp => {
      console.info(`${resp}\n`);
      return processActivities(mongodb, llBucket);
    })
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

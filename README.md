# Mongo-Gcloud parser

This scripts traverses two mongodb collections and gcloud storage to enumerate words/audios that are in mongo database but not in Gcloud. These words are outputed in a csv file named according according to the collection traversed.

## Installation

Clone and install dependecies

```
git clone git@github.com:chitezh/edu-parser.git

npm install
```

Set environment variables

```
export MONGO_URL=mongodb://us:pass@mongohost:134/db-name
export GC_PROJECT_ID=gcloud-project-id
export GC_KEY_PATH=/path/to/gcloud/key.json
```

* Ensure you have the gcloud key file

## Usage

```
npm run start -- [course]
```

Ex., for Japanese:
```
npm run start -- jp
```

## Notes

This is a simple demo script; It can be improved significantly.

## License

MIT License

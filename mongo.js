const { MongoClient } = require('mongodb');
// const csvWriter = require('csv-write-stream');
const async = require('async');

MongoClient.connect('mongodb://localhost:27017', (err, client) => {
  console.log('Connected successfully to server');

  const db = client.db('ofb');
  findDocuments(db, () => {
    client.close();
  });
});

const findDocuments = (db, callback) => {
  // Get the documents collection
  const collection = db.collection('organization');
  // Find some documents
  collection.find({ }).toArray((err, docs) => {
    console.log('Found the following records');
    docs.forEach((doc) => {
      if (doc.warehouses.length > 1) {
        console.log('=========');
        console.log(doc.warehouses);
      }
    });
    callback(docs);
  });
};

const { MongoClient } = require('mongodb');
const csvWriter = require('csv-write-stream');
const fs = require('fs');

const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Name',
    'organizationId',
    'Type',
    'Address line 1',
    'Address line 2',
    'State',
    'City',
  ],
  sendHeaders: true,
};

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('address-report.csv'));

const findDocuments = (db, callback) => {
  // Get the documents collection
  const collection = db.collection('organization');
  // Find some documents
  collection.find({ 'warehouses.address.isBillingAddress': true }).toArray((err, docs) => {
    console.log('Found the following records');
    docs.forEach((doc) => {
      let warehouse;
      const filtered = doc.warehouses.filter(w => (w.address.isBillingAddress === true));
      if (filtered.length > 0) {
        warehouse = filtered[0];
      }
      // console.log(warehouse);
      writer.write([doc.name, doc.organizationId, 'SUPPLIER', warehouse.address.addressLine1, warehouse.address.addressLine2, warehouse.address.state, warehouse.address.city]);
      // if (doc.warehouses.length > 1) {
      // console.log('=========');
      // console.log(doc.warehouses);
      // }
    });
    callback(docs);
    writer.end();
  });
};

MongoClient.connect('mongodb://localhost:27017', (err, client) => {
  console.log('Connected successfully to server');

  const db = client.db('ofb');
  findDocuments(db, () => {
    client.close();
  });
});

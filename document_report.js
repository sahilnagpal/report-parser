const elasticsearch = require('elasticsearch');
const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const dateFormat = require('dateformat');
const async = require('async');
const fs = require('fs');

const DATE_FORMAT = 'dd/mm/yyyy - hh:MM TT';
const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Client id',
    'Client name',
    'Bank name',
    'Account number',
    'Account type',
    'Verified',
    'Document Id',
    'Document Name',
    'Document status',
    'Created On',
    'Last Updated On',
    'Associated On',
  ],
  sendHeaders: true,
};

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('document_report.csv'));

const mysqlClient = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'oxyzo',
});

const esClient = new elasticsearch.Client({
  host: 'http://localhost:9200/',
});

esClient.search({
  index: 'document',
  size: 2000,
  body: {
    query: {
      bool: {
        must: [
          {
            term: {
              docType: {
                value: '6310695617769248592',
              },
            },
          },
          {
            term: {
              status: {
                value: 'VERIFIED',
              },
            },
          },
          {
            range: {
              dateCreated: {
                gte: 1512412200000,
                lt: new Date('12-20-2017').getTime(),
              },
            },
          },
        ],
      },
    },
  },
}).then((resp) => {
  const { hits } = resp.hits;
  async.eachSeries(hits, (hit, hitCallback) => {
    // eslint-disable-next-line no-underscore-dangle
    const source = hit._source;
    let clientId;
    let clientName;
    let bankName;
    let bankAccountNumber;
    let bankAccountType;
    let bankIsVerified;
    let associatedOn;

    const {
      docId: documentId, name: documentName, status: documentStatus, dateCreated, lastModified,
    } = source;

    let bankAccountId;
    source.associations.forEach((association) => {
      if (association.name === 'ENTITY_ID') {
        [clientId] = association.values;
        associatedOn = association.dateCreated;
      } else if (association.name === 'FORM_ANSWERS') {
        association.values.map(valString => JSON.parse(valString)).forEach((value) => {
          if (value.formId === '6309639617146523769') {
            [bankAccountId] = value.answers['6309639617117163638'];
          }
        });
      }
    });

    async.parallel([
      (callback) => {
        mysqlClient.query(`SELECT name FROM client WHERE clientId = ${clientId}`, (error, results) => {
          if (error) { console.error(`No client found for client id : ${clientId}`); }
          callback(null, results[0].name);
        });
      },
      (callback) => {
        mysqlClient.query(`SELECT bankName FROM bankAccount WHERE bankAccountId = ${bankAccountId}`, (error, results) => {
          if (error) { console.error(`No bankName found for bankAccountId : ${bankAccountId}`); }
          callback(null, results[0].bankName);
        });
      },
      (callback) => {
        mysqlClient.query(`SELECT accountNumber FROM bankAccount WHERE bankAccountId = ${bankAccountId}`, (error, results) => {
          if (error) { console.error(`No accountNumber found for bankAccountId : ${bankAccountId}`); }
          callback(null, results[0].accountNumber);
        });
      },
      (callback) => {
        mysqlClient.query(`SELECT bankAccountType FROM bankAccount WHERE bankAccountId = ${bankAccountId}`, (error, results) => {
          if (error) { console.error(`No bankAccountType found for bankAccountId : ${bankAccountId}`); }
          callback(null, results[0].bankAccountType);
        });
      },
      (callback) => {
        mysqlClient.query(`SELECT isVerified FROM bankAccount WHERE bankAccountId = ${bankAccountId}`, (error, results) => {
          if (error) { console.error(`No isVerified found for bankAccountId : ${bankAccountId}`); }
          callback(null, results[0].isVerified);
        });
      },
    ], (err, results) => {
      if (err) {
        console.error('err:', err);
        hitCallback(err, null);
      } else {
        [clientName, bankName, bankAccountNumber, bankAccountType, bankIsVerified] = results;
        const columns = [
          clientId,
          clientName,
          bankName,
          bankAccountNumber,
          bankAccountType,
          bankIsVerified,
          documentId,
          documentName,
          documentStatus,
          dateFormat(new Date(dateCreated), DATE_FORMAT),
          dateFormat(new Date(lastModified), DATE_FORMAT),
          dateFormat(new Date(associatedOn), DATE_FORMAT),
        ];
        writer.write(columns);
      }
    });
  }, () => {
    mysqlClient.end();
    writer.end();
  });
}, (error) => {
  console.error('Error while querying ES', error);
});

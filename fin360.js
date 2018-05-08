const elasticsearch = require('elasticsearch');
const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const dateFormat = require('dateformat');
const async = require('async');
const fs = require('fs');
const request = require('request');


const DATE_FORMAT = 'dd/mm/yyyy - hh:MM TT';
const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Client id',
    'Client name',
    'Account number',
    'Account uuid',
  ],
  sendHeaders: true,
};

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('fin360.csv'));

const mysqlClient = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'oxyzo',
});

request.get(
  'https://www.fin360.in/bank-account/api/v1/fetchBankAccountUIDs?access_token=evjjpv940gokghvidajbrnd6ka3mc31g&format=json&daysAgo=1500',
  (error, response, body) => {
    if (!error && response.statusCode === 200) {
      const d = {};
      const data = JSON.parse(body);
      data.forEach((dataPoint) => {
        if (d[dataPoint.accountNo] === undefined) {
          d[dataPoint.accountNo] = [];
        }
        d[dataPoint.accountNo].push(dataPoint.bankAccountUUID);
      });
      console.log('Response : ', d);
      console.log('size : ', Object.keys(d).length);
      async.eachSeries(Object.keys(d), (accountNumber, accountNumberCallback) => {
        console.log('bankAccountNumber: ', accountNumber);
        mysqlClient.query(`SELECT c.clientId, c.name, b.accountNumber FROM bankAccount b, client c WHERE b.clientId = c.clientId AND b.accountNumber = ${accountNumber}`, (merror, results) => {
          if (merror) {
            console.error(`merror : ${accountNumber}`);
            accountNumberCallback(null, null);
          } else {
            console.log(`${accountNumber} : `, results);
            if (results.length > 0) {
              writer.write([results[0].clientId, results[0].name, accountNumber, d[accountNumber]]);
            } else {
              writer.write(['', '', accountNumber, d[accountNumber]]);
            }
            accountNumberCallback(null, results);
          }
          // callback(null, results[0].name);
        });
      }, () => {
        console.log('uidMap: ');
        mysqlClient.end();
        writer.end();
      });
    } else {
      console.log('Error : ', error);
    }
  },
);

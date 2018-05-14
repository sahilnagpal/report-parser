const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const csvReader = require('csv-reader');
const async = require('async');

const readCsv = (callback) => {
  const data = [];
  const inputStream = fs.createReadStream('anchors.csv', 'utf8');
  inputStream
    .pipe(csvReader({ parseNumbers: true, parseBooleans: true, trim: true }))
    .on('data', (row) => {
      if (row[0].length !== 0) {
        const clientName = row[1];
        const anchorSource = row[2];
        data[anchorSource] = clientName;
      }
    })
    .on('end', () => {
      callback(null, data);
    });
};

const mapAnchors = (data, callback) => {
  const results = [];
  const mysqlClient = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'oxyzo',
  });
  async.eachSeries(Object.keys(data), (anchorSource, seriesCallback) => {
    const clientName = data[anchorSource];
    async.parallel({
      anchorSources(anchorSourcesCallback) {
        mysqlClient.query(`SELECT * FROM anchorSource WHERE sourceValue = "${anchorSource}"`, (sqlerror, sqlResults) => {
          if (sqlerror) {
            console.error('anchorSources sqlerror "', anchorSource, '": ', sqlerror);
            anchorSourcesCallback(sqlerror, null);
          } else {
            anchorSourcesCallback(null, sqlResults);
          }
        });
      },
      clients(clientsCallback) {
        const names = clientName.split('|').map(t => t.trim()).filter(t => t.length > 0);
        if (names.length === 0) {
          clientsCallback(null, []);
        } else {
          const query = names.map(name => `"${name}"`).join();
          mysqlClient.query(`SELECT * FROM client WHERE name IN (${query}) AND status = "VERIFIED"`, (sqlerror, sqlResults) => {
            if (sqlerror) {
              console.error('clients sqlerror "', clientName, '": ', sqlerror);
              clientsCallback(sqlerror, null);
            } else {
              clientsCallback(null, sqlResults);
            }
          });
        }
      },
    }, (err, sqlData) => {
      // results is now equals to: {one: 1, two: 2}
      if (err) {
        seriesCallback(err, null);
      } else {
        const { anchorSources, clients } = sqlData;
        if (clients.length === 1) {
          const { clientId, name } = clients[0];
          async.eachSeries(anchorSources, (source, updateCallback) => {
            const { anchorSourceId, anchorId, sourceValue } = source;
            mysqlClient.query(
              `UPDATE anchorSource SET sourceValue = "${clientId}", type = "CLIENT" WHERE anchorSourceId = "${anchorSourceId}"`,
              (sqlerror) => {
                if (sqlerror) {
                  console.error('anchorSource update sqlerror "', sourceValue, '": ', sqlerror);
                  results.push([anchorSource, anchorSourceId, anchorId, name, clientId, false]);
                  updateCallback(null, null);
                } else {
                  results.push([anchorSource, anchorSourceId, anchorId, name, clientId, true]);
                  updateCallback(null, null);
                }
              },
            );
          }, (updateErr) => {
            if (updateErr) {
              console.error('updateErr: ', updateErr);
            }
            seriesCallback(null, null);
          });
        } else {
          anchorSources.forEach((source) => {
            results.push([anchorSource, source.anchorSourceId, source.anchorId, clients.map(client => client.name).join('\n'), clients.map(client => client.clientId).join('\n'), false]);
          });
          seriesCallback(null, null);
        }
      }
    });
  }, (err) => {
    if (err) {
      console.error('err: ', err);
    }
    mysqlClient.end();
    callback(null, results);
  });
};


const handleResults = (result, callback) => {
  const writer = csvWriter({
    separator: ',',
    newline: '\n',
    headers: [
      'Anchor Source',
      'Anchor Source ID',
      'Anchor ID',
      'Client Name',
      'Client ID',
      'Mapped',
    ],
    sendHeaders: true,
  });
  writer.pipe(fs.createWriteStream('anchor_source_migration_result.csv'));

  result.forEach((row) => {
    writer.write(row);
  });
  writer.end();
  callback(null, `${result.length} rows written to csv`);
};


async.waterfall([
  readCsv, mapAnchors, handleResults,
], (err, results) => {
  if (err) {
    console.error('err: ', err);
  }
  console.log('res: ', results);
});

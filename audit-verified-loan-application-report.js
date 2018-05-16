const elasticsearch = require('elasticsearch');
const mysql = require('mysql');
const csvWriter = require('csv-write-stream');
const dateFormat = require('dateformat');
const async = require('async');
const fs = require('fs');
const request = require('request');


  const mysqlClient = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'oxyzo_audit',
  });
  
  const esClient = new elasticsearch.Client({
    host: 'http://localhost:9200/',
  });


  const handleResults = (result, callback) => {
    const writer = csvWriter({
      separator: ',',
      newline: '\n',
      headers: [
        'Application Id',
        'Count'
      ],
      sendHeaders: true,
    });
    writer.pipe(fs.createWriteStream('audit-verified-loan-applications.csv'));
  
    result.forEach((row) => {
      writer.write(row);
    });
    writer.end();
    callback(null, `${result.length} rows written to csv`);
  };
 const generateReport = (applicationIds, callback) => {
     const successResults = [];
    async.eachSeries(applicationIds, (loanApplicationId, updateCallback) => {
        mysqlClient.query(
            'select count(*) as auditCount from jv_snapshot where changed_properties like "%loanApplicationStatus%" and state like "% VERIFIED%" and managed_type="com.ofcredit.oxyzo.core.loanapplication.domain.LoanApplication" and state like "%' + loanApplicationId + '%"',    
                (sqlerror, sqlResults) => {
                if (sqlerror) {
                    console.error('error while fetching loan application "', loanApplicationId, '": ', sqlerror);
                    updateCallback(null, null);
                } else {
                    successResults.push([loanApplicationId, sqlResults[0].auditCount]);
                    console.log(loanApplicationId, sqlResults[0].auditCount);
                    updateCallback(null, null);
                }
            },
        );
    }, (updateErr) => {
    if (updateErr) {
        console.error('updateErr: ', updateErr);
    }
    mysqlClient.end();
    //seriesCallback(null, null);
    });
 }  
  
 esClient.search({
    index: 'oxyzo_loanapplication',
    size: 0,
    body: {
        "query": {
            "bool": {
              "must_not": [
                {
                  "term": {
                    "loanApplicationStatus": {
                      "value": "UNVERIFIED"
                    }
                  }
                },
                {
                  "nested": {
                    "path": "statusTransitionComments",
                    "query": {
                      "match": {
                        "statusTransitionComments.fromStatus": "UNVERIFIED"
                      }
                    }
                  }
                }
              ],
              "must": [
                {
                  "range": {
                    "dateCreated": {
                      "gte": "now-4M/M"
                    }
                  }
                }
              ]
            }
          },
          "aggs": {
            "loanApplications": {
              "terms": {
                "field": "applicationId",
                "size": 400
              }
            }
          }
    },
  }).then((resp) => {
    const loanApplicationIds = resp.aggregations.loanApplications.buckets.map((bucketInfo) => {
        return bucketInfo.key;
    });
    generateReport(loanApplicationIds);

  });

  
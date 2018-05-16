const elasticsearch = require('elasticsearch');
const csvWriter = require('csv-write-stream');
const dateFormat = require('dateformat');
const fs = require('fs');

const options = {
  separator: ',',
  newline: '\n',
  headers: ['ApplicationId', 'Status', 'Processing Officer', 'Client name'],
  sendHeaders: true,
};

const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('underwriting.csv'));

const host = 'http://35.194.249.217:9200/';

const client = new elasticsearch.Client({
  host,
});


client.search({
  index: 'oxyzo_loanapplication',
  size: 2000,
  body: {
    query: {
      terms: {
        loanApplicationStatus: ['UNDERWRITING', 'UNDERWRITING_QUERY'],
      },
    },
  },
}).then((resp) => {
  console.log('Total Hits : ', resp.hits.total);
  resp.hits.hits.forEach((hit) => {
    // console.log(hit);
    const rowData = [];
    const source = hit._source;
    rowData.push(source.applicationId);
    rowData.push(source.loanApplicationStatus);
    rowData.push(source.processingOfficerAccount ? source.processingOfficerAccount.name : '-');
    rowData.push(source.client.name);
    writer.write(rowData);
  });
  writer.end();
}, (error) => {
  console.error('Error while querying ES', error);
});

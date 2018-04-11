const elasticsearch = require('elasticsearch');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const dateFormat = require('dateformat');


const DATE_FORMAT = 'dd/mm/yyyy - hh:MM TT';
const options = {
  separator: ',',
  newline: '\n',
  headers: [
    'Client Id',
    'Client Name',
    'Client Pan',
    'Client Status',
    'Loan Application Id',
    'Loan Application Creation Date',
    'Loan Application Status',
    'Loan Request Id',
    'Loan Request Status',
    'Loan Request Creation Date',
    'Loan Request NBFC',
    'Loan Request Offered Amount',
    'Loan Request Interest rate',
    'Loan Request Repayment Cycle',
    'Loan Request Processing Charge'],
  sendHeaders: true,
};


const writer = csvWriter(options);
writer.pipe(fs.createWriteStream('loan_application_report.csv'));

const host = 'https://35.194.249.217:9200/';

const client = new elasticsearch.Client({
  host,
});

client.search({
  index: 'oxyzo_loanapplication',
  size: 3000,
  body: {
    query: {
      match: {
        deleted: false,
      },
    },
  },
}).then((resp) => {
  const hits = resp.hits.hits;
  hits.map((hit) => {
    // console.log(hit._source);
    const clientDetails = [];
    const source = hit._source;
    clientDetails.push(source.client.clientId);
    clientDetails.push(source.client.name);
    clientDetails.push(source.client.pan);
    clientDetails.push(source.client.clientStatus);
    clientDetails.push(source.applicationId);
    clientDetails.push(dateFormat(new Date(source.dateCreated), DATE_FORMAT));
    clientDetails.push(source.loanApplicationStatus);

    if (source.loanRequestIndices !== null) {
      source.loanRequestIndices.forEach((loanRequest) => {
        let row = [];
        row = row.concat(clientDetails);
        row.push(loanRequest.loanRequestId);
        row.push(loanRequest.status);
        row.push(dateFormat(new Date(loanRequest.dateCreated), DATE_FORMAT));
        row.push(loanRequest.nbfcName);
        row.push(loanRequest.offerLoanAmount);
        row.push(loanRequest.interestRate);
        row.push(loanRequest.repaymentCycle);
        row.push(loanRequest.processingCharge);
        writer.write(row);
      });
    } else {
      writer.write(clientDetails);
    }
  });
  writer.end();
}, (error) => {
  console.error('Error while querying ES', error);
});

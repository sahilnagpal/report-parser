var elasticsearch = require('elasticsearch');
var csvWriter = require('csv-write-stream')
var fs = require('fs');

var options = {
  separator: ',',
  newline: '\n',
  headers: ["Date created", "Client name", "Pan", "Status",
   "Offered amount", "Last Status date", "Last status person", "Sales person"],
  sendHeaders: true
};

var writer = csvWriter(options);
writer.pipe(fs.createWriteStream('loan_application_report.csv'))

var host = 'http://localhost:9200/';

var client = new elasticsearch.Client({
	host: host,
});

client.search({
	index: 'oxyzo_loanapplication',
	size: 2000,
	body: {
		query: {
			match: {
				deleted: false
			}
		}
	}
}).then(function(resp) {
	var hits = resp.hits.hits;
	hits.map(function(hit) {
		//console.log(hit._source);
		var clientDetails = [];
		var source = hit._source;
		clientDetails.push(new Date(source.dateCreated));
		clientDetails.push(source.client.name);
		clientDetails.push(source.client.pan);
		clientDetails.push(source.loanApplicationStatus);
		if (source.loanRequestIndices !== null) {
			var amounts = source.loanRequestIndices.map(function(loanRequest) {
				return loanRequest.nbfcName + " : " + (loanRequest.offerLoanAmount != null ? loanRequest.offerLoanAmount : loanRequest.loanAmount);
			});
			clientDetails.push(amounts.join(','));
		} else {
			clientDetails.push("");
		}
		if (source.statusTransitionComments != null && source.statusTransitionComments.length > 0) {
			var comment = source.statusTransitionComments[source.statusTransitionComments.length - 1];
			clientDetails.push(new Date(comment.dateCreated));
			clientDetails.push(comment.lastModifiedByName);
		} else {
			clientDetails.push("");
			clientDetails.push("");
		}
		if (source.salesPersonAccount != null) {
			clientDetails.push(source.salesPersonAccount.name);	
		} else {
			clientDetails.push("");	
		}
		writer.write(clientDetails);
	});
	writer.end()
}, function(error) {
	console.error('Error while querying ES', error);
});

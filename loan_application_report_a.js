var elasticsearch = require('elasticsearch');
var csvWriter = require('csv-write-stream')
var dateFormat = require('dateformat');
var fs = require('fs');

var options = {
	separator: ',',
	newline: '\n',
	headers: ["Date created", "Client name", "ApplicationId", "Status", "Transitions"],
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
		sort: ["dateCreated"],
		query: {
			bool: {
				must: {
					nested: {
						path: "statusTransitionComments",
						query: {
							bool: {
								must: [
									{
										terms: {
											"statusTransitionComments.status": [
												"PROCESSING",
												"UNDERWRITING"
											]
										}
									},
									{
										range: {
											"statusTransitionComments.dateCreated": {
												gte: 1512412200000,
												lte: 1513621800000
											}
										}
									}
								]
							}
						},
						inner_hits: {
							sort: ["statusTransitionComments.dateCreated"]
						}
					}
				}
			}
		}
	}
}).then(function (resp) {
	console.log("Total Hits : ", resp.hits.total)
	var hits = resp.hits.hits;
	hits.map(function (hit) {
		// console.log(hit);
		var rowData = [];
		var source = hit._source;
		rowData.push(dateFormat(new Date(source.dateCreated), "dd/mm/yyyy - hh:MM TT"));
		rowData.push(source.client.name);
		rowData.push(source.applicationId);
		rowData.push(source.loanApplicationStatus);

		var transitions = '';
		hit.inner_hits.statusTransitionComments.hits.hits.map(function (innerHit) {
			if (transitions.length != 0) {
				transitions += "\n";
			}
			transitions += innerHit._source.status;
			transitions += " - ";
			transitions += dateFormat(new Date(innerHit._source.dateCreated), "dd/mm/yyyy - hh:MM TT");
		});
		rowData.push(transitions);
		writer.write(rowData);
	});
	writer.end()
}, function (error) {
	console.error('Error while querying ES', error);
});

var elasticsearch = require('elasticsearch');
var csvWriter = require('csv-write-stream')
var fs = require('fs');

var options = {
  separator: ',',
  newline: '\n',
  headers: ["Client Name", "Pan", "Status", "Industry type", "Client Id"],
  sendHeaders: true
};

var writer = csvWriter(options);
writer.pipe(fs.createWriteStream('out.csv'))

var host = 'localhost:9200';

var client = new elasticsearch.Client({
	host: host,
});

client.search({
	index: 'oxyzo_client',
	size: 1000,
	body: {
		query: {
			match: {
				status: 'VERIFIED'
			}
		}
	}
}).then(function(resp) {
	var hits = resp.hits.hits;
	hits.map(function(hit) {
		//console.log(hit._source);
		var clientDetails = [];
		var source = hit._source;
		clientDetails.push(source.name);
		clientDetails.push(source.pan);
		clientDetails.push(source.status);
		clientDetails.push(source.natureOfBusiness);
		clientDetails.push("'" + source.clientId);
		writer.write(clientDetails);
	});
	writer.end()
}, function(error) {
	console.error('Error while querying ES', error);
});

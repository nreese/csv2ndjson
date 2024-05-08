const Fs = require('fs');
const CsvReadableStream = require('csv-reader');

const OUTPUT_FILE = './ndjson.txt';
const INPUT_FILE = './in.csv';
const IGNORE_KEYS = ['_id', '_index', '_score'];
const KEY_FORMATTERS = {
  '@timestamp': (value) => {
    const date = new Date(Date.parse(value));
    return date.toISOString();
  },
  'destination.bytes': (value) => {
    return parseInt(value.replaceAll(',', ''), 10);
  },
  'source.bytes': (value) => {
    return parseInt(value.replaceAll(',', ''), 10);
  }
}

const inputStream = Fs.createReadStream(INPUT_FILE, 'utf8');
let headers;
const documents = [];
inputStream
	.pipe(new CsvReadableStream({ parseNumbers: true, parseBooleans: true, trim: true }))
	.on('data', function (row) {
      if (!headers) {
        headers = row;
        return;
      }
      const document = {};
      row.forEach((value, index) => {
        const key = headers[index];
        if (!IGNORE_KEYS.includes(key) && value !== '-') {
          document[key] = KEY_FORMATTERS[key]
            ? KEY_FORMATTERS[key](value)
            : value;
        }
      });
      documents.push(document);
	})
	.on('end', function () {
	    console.log('found documents: ', documents.length);
      const writeStream = Fs.createWriteStream(OUTPUT_FILE);
      documents.forEach(document => {
        writeStream.write(`${JSON.stringify(document)}\n`, 'utf8');
      });
      writeStream.on('finish', () => {
        console.log(`Wrote ${documents.length} documents to Newline Delimited json to ${OUTPUT_FILE}`);
      });
      writeStream.end();
	});
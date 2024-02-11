const { Storage } = require("@google-cloud/storage");
const { BigQuery } = require("@google-cloud/bigquery");
const csv = require("csv-parser");

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);
    const bq = new BigQuery();

    dataFile.createReadStream()
        .on('error', (err) => {
            console.error("Error reading file:", err);
        })
        .pipe(csv())
        .on('data', async (row) => {
            // Transform the data
            transformRow(row);
            
            // Write transformed data to BigQuery
            await writeToBigQuery(row, bq);
        })
        .on('end', () => {
            console.log(`End of file processing: ${file.name}`);
        });
}

// Helper function to transform the data
function transformRow(row) {
    // Convert numeric fields
    for (let key in row) {
        if (row[key] === "-9999") {
            // Replace missing values with null
            row[key] = null;
        } else if (!isNaN(row[key])) {
            // Convert numeric fields to numbers
            row[key] = parseFloat(row[key]);
            // Transform specific fields
            if (key === "airtemp" || key === "dewpoint" || key === "pressure" || key === "windspeed" || key === "precip1hour" || key === "precip6hour") {
                row[key] /= 10; // Divide by 10
            }
        }
    }
}

// Helper function to write the row to BigQuery
async function writeToBigQuery(row, bq) {
    const datasetId = 'your_dataset_id';
    const tableId = 'your_table_id';
    const table = bq.dataset(datasetId).table(tableId);

    try {
        // Insert row into BigQuery
        await table.insert(row);
        console.log(`Row inserted into BigQuery: ${JSON.stringify(row)}`);
    } catch (err) {
        console.error(`Error inserting row into BigQuery: ${err}`);
    }
}

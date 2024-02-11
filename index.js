const { Storage } = require("@google-cloud/storage");
const csv = require("csv-parser");

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
        .on('error', (err) => {
            console.error("Error reading file:", err);
        })
        .pipe(csv())
        .on('data', (row) => {
            // Transform the data
            transformRow(row);
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

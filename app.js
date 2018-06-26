const { MongoClient } = require('mongodb');
const async = require('async');

const custAddData = require('./m3-customer-address-data.json');
const custData = require('./m3-customer-data.json');

let startTime = new Date();

let argv = parseInt(process.argv[2]);
let tasks = [];
let customers = [];
let recordLength = custData.length;
let queries = recordLength / argv;

console.log(`Customer count ${recordLength}, will result in ${queries} queries`);

MongoClient.connect('mongodb://localhost:27017', (err, client) => {
    if (err) return process.exit(1);

    const db = client.db('module3');

    console.log('database connection successful');

    const collection = db.collection('customers');

    collection.remove();

    let batchTasks = (batch_number) => {
        return function (callback) {
            let start = batch_number * argv;
            let end = start + argv;

            for (let i=start; i<end; i++) {
                const customer = Object.assign(custData[i], custAddData[i]);
                customers.push(customer);
            }

            collection.insert(customers, (err, results) => {
                if (err) return process.exit(1);
                callback(null, results);
            });
        };
    };

    for (let i=0; i<queries; i++) {
        tasks.push(batchTasks(i));
    }

    async.parallel(tasks, (error, results) => {
      if (error) console.error(error);
      console.log('successful parallel tasks');
      return process.exit();
    })
});

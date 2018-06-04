const express = require('express')
var bodyParser = require('body-parser')
var os = require('os')
const sql = require('mssql')
const app = express()

var server = 'localhost';
var database = 'epglobal';
var username = 'node1';
var password = 'hello#123';
var payload;

const config = {
    server : server,
    database : database,
    user : username,
    password : password
}

var request;

sql.connect(config, function (err) {
    
    if (err) console.log(err);

    // create Request object
    request = new sql.Request();
       
    // query to the database and get the records
});




function getOrigin(id) {
         var origin;
           
        // query to the database and get the records
        request.query(`select origin from customerorigin where id = ${id}`, function (err, recordset) {
            
            if (err) console.log(err)

            // send records as a response
            try {
                console.log(recordset.recordset[0]['origin'])
                origin = recordset.recordset[0]['origin'];
            }
            catch(err) {
                origin = os.hostname;
            }
            
            if (origin === os.hostname) {
                sendMessage(payload)
                console.log(payload)
                //console.log("Unique Message Sent!!")  
            }
            //console.log(origin);
        });
}

var AWS = require('aws-sdk');

var sns = new AWS.SNS({region: "us-west-2"});

function sendMessage(message) {
    var params = {
        Message: JSON.stringify(message),
        MessageStructure: 'json',
        TopicArn: 'arn:aws:sns:us-west-2:107995894928:epglobal'
    };
    sns.publish(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
    });
}

app.get('/', (req, res) => res.send('Hello SNS sender go to /message!'))
app.get('/message', (req, res) => {
    //console.log("URL: ", req.url)

    payload = {
        default: JSON.stringify(req.query),
        sqs: JSON.stringify(req.query)
    }
    //console.log("query is:", req.query)
    console.log(req.query['uid'])
    getOrigin(req.query['uid']);
})

app.listen(8080, () => console.log('SNS app listening on port 8080!'))
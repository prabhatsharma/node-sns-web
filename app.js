const express = require('express')
var bodyParser = require('body-parser')
const app = express()

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
    console.log("URL: ", req.url)

    var payload = {
        default: JSON.stringify(req.query),
        sqs: JSON.stringify(req.query)
    }
    console.log("query is:", req.query)
    sendMessage(payload)
})

app.listen(8080, () => console.log('SNS app listening on port 8080!'))
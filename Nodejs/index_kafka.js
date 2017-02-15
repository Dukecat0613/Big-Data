/**
* @Author: Hang Wu <Dukecat>
* @Date:   2017-02-11T22:33:57-05:00
* @Email:  wuhang0613@gmail.com
* @Last modified by:   Dukecat
* @Last modified time: 2017-02-15T14:48:32-05:00
*/


// - get command line arguments
var argv = require('minimist')(process.argv.slice(2));
var port = argv['port'];
var kafka_topic = argv['kafka_topic'];
var kafka_broker = argv['kafka_broker'];
// - setup dependency instances

var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);

// - setup kafka client
var kafka=require('kafka-node');
var Consumer=kafka.HighLevelConsumer;
var Client= kafka.Client;
var client = new Client(kafka_broker);

console.log('Creating a kafka consumer');

var consumer =  new Consumer(
  client,
  [{topic : kafka_topic}]
);

console.log('Subscribing to kafka topic %s', kafka_topic);

consumer.on('message', function (message) {
      console.log('message received %s', JSON.stringify(message));
      io.sockets.emit('data', JSON.stringify(message));

});

consumer.on ('error', function (err){
  console.log('error', err);
});

// - setup webapp routing
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "http://localhost:3000");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});
app.use(express.static(__dirname + '/public'));

app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));

server.listen(port, function () {
    console.log('Server started at port %d.', port);
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting kafka consumer');
    consumer.close();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);

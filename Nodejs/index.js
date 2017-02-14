/**
* @Author: Hang Wu <Dukecat>
* @Date:   2017-02-11T22:33:57-05:00
* @Email:  wuhang0613@gmail.com
* @Last modified by:   Dukecat
* @Last modified time: 2017-02-14T16:54:57-05:00
*/


// - get command line arguments
var argv = require('minimist')(process.argv.slice(2));
var port = argv['port'];
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];
var subscribe_topic = argv['subscribe_topic'];

// - setup dependency instances
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);

// - setup redis client
var redis = require('redis');
console.log('Creating a redis client');
var redisclient = redis.createClient(redis_port, redis_host);

console.log('Subscribing to redis topic %s', subscribe_topic);

redisclient.subscribe(subscribe_topic);


redisclient.on('message', function (channel, message) {
    if (channel == subscribe_topic) {
        console.log('message received %s', message);
        io.sockets.emit('data', message);
    }
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
    console.log('Quitting redis client');
    redisclient.quit();
    console.log('Shutting down app');
    process.exit();
};
// -release resource     
process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);

/**
* @Author: Hang Wu <Dukecat>
* @Date:   2017-02-11T23:01:55-05:00
* @Email:  wuhang0613@gmail.com
* @Last modified by:   Dukecat
* @Last modified time: 2017-02-15T23:18:59-05:00
*/

$(function () {

    var data_points = [];

    $("#chart").height($(window).height() - $("#header").height() * 2);

    $(document.body).on('click', '.stock-label', function () {

        "use strict";
        var symbol = $(this).text();
        $.ajax({

            url: 'http://localhost:5000/' + symbol + '/delete',
            type: 'POST'

        });

        $(this).hide();
        var i = getSymbolIndex(symbol, data_points);
        data_points.splice(i, 1);
        console.log(data_points);
    });

    $("#add-stock-button").click(function () {
        "use strict";
        var symbol = $("#stock-symbol").val();

        $.ajax({
            url: 'http://localhost:5000/' + symbol + '/add',
            type: 'POST'
        });

        $("#stock-symbol").val("");
        data_points.push({
            values: [],
            key: symbol
        });

        $("#stock-list").append(
            "<a class='stock-label list-group-item small'><span style='color:red'>" + symbol + "</span></a>"
        );

        console.log(data_points);
    });

    function getSymbolIndex(symbol, array) {
        "use strict";
        for (var i = 0; i < array.length; i++) {
            if (array[i].key == symbol) {
                return i;
            }
        }
        return -1;
    }

    var chart = nv.models.lineChart()
        .interpolate('monotone')
        .margin({
            bottom: 100
        })
        .useInteractiveGuideline(true)
        .showLegend(true)
        .color(d3.scale.category10().range());

    chart.xAxis
        .axisLabel('Time')
        .tickFormat(formatDateTick);

    chart.yAxis
        .axisLabel('Price($)');

    nv.addGraph(loadGraph);

    function loadGraph() {
        "use strict";
        d3.select('#chart svg')
            .datum(data_points)
            .transition()
            .duration(5)
            .call(chart);

        nv.utils.windowResize(chart.update);
        return chart;
    }

    function newDataCallback(message) {
        "use strict";
        var parsed = JSON.parse(message)['value'];
        parsed = JSON.parse(parsed);
        var timestamp = parsed['TimeStamp'];
        var average = parsed['AveragePrice'];
        var symbol = parsed['StockSymbol'];
        var point = {};
        point.x = timestamp;
        point.y = average;

        console.log(point);

        var i = getSymbolIndex(symbol, data_points);

        data_points[i]["values"].push(point);
        if (data_points[i]["values"].length > 100) {
            data_points[i]["values"].shift();
        }
        loadGraph();
    }

    function formatDateTick(time) {
        "use strict";
        var date = new Date(time * 1000);
        return d3.time.format('%H:%M:%S')(date);
    }

    var socket = io();

    // - Whenever the server emits 'data', update the flow graph
    socket.on('data', function (data) {

    	newDataCallback(data);
    });
});

app.directive('timeSeriesChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/time-series-chart.html',
		scope : {
			control: "="
		},
		link : function(scope, element, attrs) {
		    scope.element = element;
            scope.title = "";
			scope.type = "line";
			scope.unstack = false;
			scope.series_names = [];
            scope.value_names = [];
			scope.unit = "";
            scope.values = [];
            scope.series = [];

            scope.init = function(options, data) {
                scope.options = options;
                scope.title = options.title || '';
                scope.type = options.type || 'line';
                scope.unstack = options.unstack || false;
                scope.series_names = options.series_names || [];
                scope.value_names = options.value_names || [];
                scope.unit = options.unit || '';
                scope.height = options.height || 300;
                scope.y_range = options.y_range;


                scope.values = [];
                var palette = new Rickshaw.Color.Palette();
                scope.series = scope.series_names.map(function(name){return{name: name, color: palette.color()};});

                scope.timeBase = data[0] == null ? Date.now() / 1000 : data[0].timestamp / 1000;
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: scope.type,
                    stroke: true,
                    height: scope.height,
                    preserve: true,
                    interpolation: 'linear',
                    series: new Rickshaw.Series.FixedDuration(scope.series, undefined, {
                        timeInterval: 1000, // TODO : fixed slideDUration
                        maxDataPoints: 60,
                        timeBase: scope.timeBase
                    })
                });

                if(scope.y_range) {
                    scope.graph.min = scope.y_range[0];
                    scope.graph.max = scope.y_range[1];
                }

                if(scope.unstack) {
                    scope.graph.renderer.unstack = true;
                    scope.graph.renderer.offset = 'zero';
                }

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: scope.graph,
                    yFormatter: function(y){
                      return y + " " + scope.unit;
                    },
                    xFormatter: function(x) {return new Date(x * 1000).toString();}
                });

                var legend = new Rickshaw.Graph.Legend( {
                    graph: scope.graph,
                    element: element.context.querySelector('.chart-legend')
                });

                var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
                    graph: scope.graph,
                    legend: legend
                });

                var xAxis = new Rickshaw.Graph.Axis.X( {
                    graph: scope.graph,
                    ticksTreatment: 'glow',
                    ticks: 5,
                    tickFormat: function(d) {return d3.time.format("%H:%M:%S")(new Date(d * 1000));}
                } );
                xAxis.render();

                var yAxis = new Rickshaw.Graph.Axis.Y({
                    graph: scope.graph,
                    tickFormat: function(y){
                        return y + " " + scope.unit;
                    },
                    ticksTreatment: 'glow',
                    ticks: 2,
                });
                yAxis.render();

                scope.addAll(data);
            };

           scope.add = function(event, render) {
                // Upate series
                var data = {};
                for (var i = 0; i < scope.series_names.length; i++) {
                    var name = scope.series_names[i];
                    if(event.data[name]) {
                        data[name] = event.data[name];
                    }
                }

                var time = Math.round(event.timestamp / 1000);
                scope.graph.series.addData(data, time);

                // Update values
                scope.values = [];
                for (var i = 0; i < scope.value_names.length; i++) {
                    var name = scope.value_names[i];
                    var value = event.data[name];
                    scope.values.push({name: name, value: value});
                }

                if(render) {
                    scope.graph.render();
                }
            };

            scope.addAll = function(events) {
                 $(events).each(function(index, event){scope.add(event, false);});
                 scope.graph.render();
            };

            scope.clearData = function() {
                scope.values = [];
                scope.graph.element.removeAllListeners();
                $(scope.element.context.querySelector('.chart')).empty();
                $(scope.element.context.querySelector('.chart-legend')).empty();
                scope.init(scope.options, []);
            };

            scope.control.init = function(options, data) { scope.init(options, data)};
            scope.control.add = function(event) {scope.add(event, true);};
            scope.control.addAll = function(events) {scope.addAll(events);};
            scope.control.clearData = function() { scope.clearData()};
            scope.control.title = function(title) { scope.title = title};
        }
	};
});
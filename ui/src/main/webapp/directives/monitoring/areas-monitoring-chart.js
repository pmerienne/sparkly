app.directive('areasMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/monitoring-chart.html',
		scope : {
			clusterid : "=",
			componentid: "=",
            componentname : "=",
			monitoring: "="
		},
		link : function(scope, element, attrs) {
            var palette = new Rickshaw.Color.Palette();

            var series = [];
            for (var i = 0; i < scope.monitoring.values.length; i++) {
               series.push({name: scope.monitoring.values[i], color: palette.color()});
            }

            scope.initGraph = function(timeBase) {
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: 'area',
                    stroke: true,
                    height: 300,
                    preserve: true,
                    interpolation: 'linear',
                    series: new Rickshaw.Series.FixedDuration(series, undefined, {
                        timeInterval: 1000, // TODO : fixed slideDUration
                        maxDataPoints: 60,
                        timeBase: timeBase
                    })
                });
                scope.graph.renderer.unstack = true;
                scope.graph.renderer.offset = 'zero';

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: scope.graph,
                    yFormatter: function(y){
                      return y + " " + scope.monitoring.unit;
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
                        return y + " " + scope.monitoring.unit;
                    },
                    ticksTreatment: 'glow',
                    ticks: 2,
                });
                yAxis.render();

                scope.graph.render();
            };

            scope.dataSource = new MonitoringDataSource(scope.clusterid, scope.componentid, scope.monitoring.name);
            scope.dataSource.listen(function(event){
                scope.addEvent(event);
                scope.graph.render();
            }, function(pastData) {
                var timeBase = pastData[0] == null ? Date.now() / 1000 : pastData[0].timestamp / 1000;
                scope.initGraph(timeBase);

                $(pastData).each(function(index, event){scope.addEvent(event);});
                scope.graph.render();
            });

            scope.addEvent = function(event) {
                var time = Math.round(event.timestamp / 1000);

                var data = {};
                for (var i = 0; i < scope.monitoring.values.length; i++) {
                    if(event.data[scope.monitoring.values[i]]) {
                        data[scope.monitoring.values[i]] = event.data[scope.monitoring.values[i]];
                    }
                }
                scope.graph.series.addData(data, time);

                scope.primaryValues = [];
                for (var i = 0; i < scope.monitoring.primaryValues.length; i++) {
                    var name = scope.monitoring.primaryValues[i];
                    var value = event.data[name];
                    scope.primaryValues.push({name: name, value: value});
                }
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });
        }
	};
});
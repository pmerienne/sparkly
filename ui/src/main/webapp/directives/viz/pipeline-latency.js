app.directive('pipelineLatencyViz', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/pipeline-latency.html',
		scope : {
			clusterId : "=cluster"
		},
		link : function(scope, element, attrs) {

            scope.initGraph = function(timeBase) {
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: 'area',
                    stroke: true,
                    height: 300,
                    preserve: true,
                    series: new Rickshaw.Series.FixedDuration([{ name: 'Scheduling delay (ms)', color: '#34495E' }, {name: 'Processing delay (ms)', color: '#5DADE2'}], undefined, {
                        timeInterval: 1000, // TODO : Hard coded batch duration
                        maxDataPoints: 60,
                        timeBase: timeBase
                    })
                });

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: scope.graph,
                    yFormatter: function(y) { return Math.floor(y) + " ms" },
                    xFormatter: function(x) {return new Date(x * 1000).toString();}
                });

                var legend = new Rickshaw.Graph.Legend( {
                    graph: scope.graph,
                    element: element.context.querySelector('.chart-legend')
                });
                var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
                    graph: scope.graph,
                    legend: legend
                } );

                var xAxis = new Rickshaw.Graph.Axis.X( {
                    graph: scope.graph,
                    ticksTreatment: 'glow',
                    ticks: 5,
                    tickFormat: function(d) {return d3.time.format("%H:%M:%S")(new Date(d * 1000));}
                } );
                xAxis.render();

                var yAxis = new Rickshaw.Graph.Axis.Y({
                    graph: scope.graph,
                    tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                    ticksTreatment: 'glow'
                });
                yAxis.render();

                scope.graph.render();
            };

            scope.dataSource = new VisualizationDataSource(scope.clusterId, "pipeline-latency");

            scope.dataSource.listen(function(event){
                scope.addEvent(event);
                scope.graph.render();
            }, function(pastData) {
                var timeBase = pastData[0] == null ? Date.now() / 1000 : pastData[0].timestampMs / 1000;
                scope.initGraph(timeBase);

                $(pastData).each(function(index, event){scope.addEvent(event);});
                scope.graph.render();
            });

            scope.addEvent = function(event) {
                var time = event.timestampMs / 1000;
                var data = {
                    'Scheduling delay (ms)': event.data.schedulingDelay,
                    'Processing delay (ms)': event.data.processingDelay
                };
                scope.graph.series.addData(data, time);
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });
        }
	};
});
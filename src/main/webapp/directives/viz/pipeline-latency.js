app.directive('pipelineLatencyViz', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/pipeline-latency.html',
		scope : {
			clusterId : "=cluster"
		},
		link : function(scope, element, attrs) {
            scope.dataSource = new VisualizationDataSource(scope.clusterId, "pipeline-latency");
            scope.dataSource.listen(function(event){
                var time = event.timestampMs / 1000;
                var data = {
                    'Scheduling delay (ms)': event.data.schedulingDelay,
                    'Processing delay (ms)': event.data.processingDelay
                };
                scope.graph.series.addData(data, time);
                scope.graph.render();
            });

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

	        var palette = new Rickshaw.Color.Palette();

            scope.graph = new Rickshaw.Graph({
                element: element.context.querySelector('.chart'),
                renderer: 'area',
                stroke: true,
                height: 300,
	            preserve: true,
                series: new Rickshaw.Series.FixedDuration([{ name: 'Scheduling delay (ms)', color: palette.color() }, {name: 'Processing delay (ms)', color: palette.color()}], undefined, {
                    timeInterval: 1000, // TODO : Hard coded batch duration
                    maxDataPoints: 60
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
        }
	};
});
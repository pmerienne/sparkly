app.directive('pythiaVisualizationThroughputvisualization', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/stream-throughput.html',
		scope : {
			clusterid : "=",
			configuration: "="
		},
		link : function(scope, element, attrs) {
		    element.context.querySelector('.chart-title').innerHTML = scope.configuration.name;

            scope.dataSource = new VisualizationDataSource(scope.clusterid, scope.configuration.id);
            scope.dataSource.listen(function(event){
                var time = event.timestampMs / 1000;
                var data = {'Throughput': event.data.throughput};
                scope.lastThroughput = Math.round(event.data.throughput);
                scope.graph.series.addData(data, time);
                scope.graph.render();
            });

            scope.lastThroughput = 0;

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            scope.graph = new Rickshaw.Graph({
                element: element.context.querySelector('.chart'),
                renderer: 'line',
                stroke: true,
                height: 300,
	            preserve: true,
	            interpolation: 'linear',
                series: new Rickshaw.Series.FixedDuration([{ name: 'Throughput', color: '#65b9ac' }], undefined, {
                    timeInterval: 1000, // TODO : Hard coded batch duration
                    maxDataPoints: 60
                })
            });

            var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                graph: scope.graph,
                yFormatter: function(y) { return Math.floor(y) + " instance/s" },
                xFormatter: function(x) {return new Date(x * 1000).toString();}
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
                ticks: 2,
                tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                ticksTreatment: 'glow'
            });
            yAxis.render();

            scope.graph.render();
        }
	};
});
app.directive('pipelineMemoryViz', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/pipeline-memory.html',
		scope : {
			clusterId : "=cluster"
		},
		link : function(scope, element, attrs) {
            scope.dataSource = new VisualizationDataSource(scope.clusterId, "pipeline-memory");

            scope.dataSource.listen(function(event){
                var time = event.timestampMs / 1000;
                var data = {
                    'Memory used (MB)': event.data["memory.used"] / 1024 / 1024,
                    'Memory committed (MB)': event.data["memory.committed"] / 1024 / 1024,
                    'Max Memory (MB)': event.data["memory.max"]  / 1024 / 1024
                };
                scope.graph.series.addData(data, time);
                scope.graph.render();
            });

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
                series: new Rickshaw.Series.FixedDuration([{ name: 'Memory used (MB)', color: '#73c03a'}, { name: 'Memory committed (MB)', color: '#65b9ac'}, {name: 'Max Memory (MB)', color: '#cb513a'}], undefined, {
                    timeInterval: 10000,
                    maxDataPoints: 60
                })
            });

            var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                graph: scope.graph,
                yFormatter: function(y) { return Math.floor(y) + " MB" },
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
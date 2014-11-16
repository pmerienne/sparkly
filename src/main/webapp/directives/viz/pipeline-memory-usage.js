app.directive('pipelineMemoryViz', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/pipeline-memory.html',
		scope : {
			clusterId : "=cluster"
		},
		link : function(scope, element, attrs) {
            function humanSize(bytes) {
                var thresh = 1000;
                if(bytes < thresh) return bytes.toFixed(1) + ' B';
                var units = ['KB', 'MB', 'GB','TB','PB','EB','ZB','YB'];
                var u = -1;
                do {
                    bytes /= thresh;
                    ++u;
                } while(bytes >= thresh);
                return bytes.toFixed(1)+' '+units[u];
            };

            scope.dataSource = new VisualizationDataSource(scope.clusterId, "pipeline-memory");
            scope.dataSource.listen(function(event){
                var time = event.timestampMs / 1000;
                var data = {
                    'Memory used': event.data["memory.used"],
                    'Memory committed': event.data["memory.committed"],
                    'Max Memory': event.data["memory.max"]
                };
                scope.graph.series.addData(data, time);
                scope.graph.render();

                scope.currentUsage = humanSize(event.data["memory.used"]) + "/" + humanSize(event.data["memory.max"]);
                scope.currentUsagePercent = Math.round(100 * event.data["memory.used"] / event.data["memory.max"]);
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
                series: new Rickshaw.Series.FixedDuration([{ name: 'Memory used', color: '#73c03a'}, { name: 'Memory committed', color: '#65b9ac'}, {name: 'Max Memory', color: '#cb513a'}], undefined, {
                    timeInterval: 10000,
                    maxDataPoints: 60
                })
            });

            var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                graph: scope.graph,
                yFormatter: function(y) { return humanSize(y)},
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
                tickFormat: function(y){return humanSize(y);},
                ticksTreatment: 'glow',
                ticks: 2,
            });
            yAxis.render();

            scope.graph.render();
        }
	};
});
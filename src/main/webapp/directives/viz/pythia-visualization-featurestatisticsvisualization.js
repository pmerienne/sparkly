app.directive('pythiaVisualizationFeaturestatisticsvisualization', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/feature-statistics.html',
		scope : {
			clusterid : "=",
			configuration: "="
		},
		link : function(scope, element, attrs) {
            scope.dataSource = new VisualizationDataSource(scope.clusterid, scope.configuration.id);
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
                    'Min' : event.data["min"],
                    '25%' : event.data["quantile 0.25"],
                    '50%' : event.data["quantile 0.50"],
                    '75%' : event.data["quantile 0.75"],
                    '90%' : event.data["quantile 0.90"],
                    '99%' : event.data["quantile 0.99"],
                    'Max' : event.data["max"],
                };

                scope.graph.series.addData(data, time);
                scope.currentMean = event.data["mean"].toFixed(1);
                scope.currentStd = event.data["std"].toFixed(1);
                scope.currentMissing = (100 * event.data["missing"] / event.data["count"]).toFixed(1);
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            scope.initGraph = function(timeBase) {
                var palette = new Rickshaw.Color.Palette();
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: 'area',
                    stroke: true,
                    height: 300,
                    preserve: true,
                    interpolation: 'linear',
                    series: new Rickshaw.Series.FixedDuration([
                        {name: 'Max', color: '#5DADE2'},
                        {name: '99%', color: '#569CCC'},
                        {name: '90%', color: '#4F8CB6'},
                        {name: '75%', color: '#497BA0'},
                        {name: '50%', color: '#426A8A'},
                        {name: '25%', color: '#3B5A74'},
                        {name: 'Min', color: '#34495E'}
                    ], undefined, {
                        timeInterval: 1000, // TODO : Hard coded batch duration
                        maxDataPoints: 60,
                        timeBase: timeBase
                    })
                });
                scope.graph.renderer.unstack = true;
                scope.graph.renderer.offset = 'zero';

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: scope.graph,
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
                    tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                    ticksTreatment: 'glow'
                });
                yAxis.render();

                scope.graph.render();
            };
        }
	};
});
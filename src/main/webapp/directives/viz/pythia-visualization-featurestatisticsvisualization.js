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
                scope.graph.render();

                scope.currentMean = event.data["mean"].toFixed(1);
                scope.currentStd = event.data["std"].toFixed(1);
                scope.currentMissing = (100 * event.data["missing"] / event.data["count"]).toFixed(1);
            });

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            // var missingScale = d3.scale.linear().domain([0, 100]);
            // var baseScale = d3.scale.linear().range(missingScale.range());

	        var palette = new Rickshaw.Color.Palette();
            scope.graph = new Rickshaw.Graph({
                element: element.context.querySelector('.chart'),
                renderer: 'area',
                stroke: true,
                height: 300,
	            preserve: true,
	            interpolation: 'linear',
                series: new Rickshaw.Series.FixedDuration([
                    {name: 'Max', color: '#65B9AC'},
                    {name: '99%', color: '#58A196'},
                    {name: '90%', color: '#4B8A81'},
                    {name: '75%', color: '#3F736B'},
                    {name: '50%', color: '#325C56'},
                    {name: '25%', color: '#254540'},
                    {name: 'Min', color: '#192E2B'}
                ], undefined, {
                    timeInterval: 1000, // TODO : Hard coded batch duration
                    maxDataPoints: 60
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
        }
	};
});
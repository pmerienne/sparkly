app.directive('pythiaVisualizationCategoricalvisualization', function(VisualizationDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/visualization/categorical.html',
		scope : {
			clusterid : "=",
			configuration: "="
		},
		link : function(scope, element, attrs) {
	        var palette = new Rickshaw.Color.Palette();

		    var categories = ["Missing"];
		    var series = [{name: "Missing", color: "gray"}];

            scope.dataSource = new VisualizationDataSource(scope.clusterid, scope.configuration.id);
            scope.dataSource.listen(function(event){
                scope.addEvent(event);
                scope.updateLegend();
                scope.graph.render();
            }, function(pastData) {
                var timeBase = pastData[0] == null ? Date.now() / 1000 : pastData[0].timestampMs / 1000;
                scope.initGraph(timeBase);

                $(pastData).each(function(index, event){scope.addEvent(event);});
                scope.updateLegend();
                scope.graph.render();
            });

            scope.addEvent = function(event) {
                var time = event.timestampMs / 1000;
                var total = event.data['$TOTAL$'];
                var missing = 100 * event.data['$MISSING_FEATURE$'] / total;
                delete event.data['$TOTAL$'];
                delete event.data['MISSING_FEATURE$'];

                var data = {"Missing": missing};
                for(category in event.data) {
                    data[category] = 100 * event.data[category] / total;

                    if($.inArray(category, categories) < 0) {
                        categories.push(category);
                        series.push({name: category, color: palette.color()});
                    }
                };

                scope.graph.series.addData(data, time);
            };

            scope.updateLegend = function() {
                element.context.querySelector('.chart-legend').innerHTML = "";
                var legend = new Rickshaw.Graph.Legend({
                    graph: scope.graph,
                    element: element.context.querySelector('.chart-legend')
                });
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            scope.initGraph = function(timeBase) {
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: 'area',
                    stroke: true,
                    height: 300,
                    preserve: true,
                    series: new Rickshaw.Series.FixedDuration(series, undefined, {
                        timeInterval: 1000, // TODO : Hard coded batch duration
                        maxDataPoints: 60,
                        timeBase: timeBase
                    })
                });

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: scope.graph,
                    yFormatter: function(y) { return Math.floor(y) + " %" },
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
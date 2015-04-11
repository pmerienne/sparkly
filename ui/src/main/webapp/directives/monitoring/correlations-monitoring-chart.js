app.directive('correlationsMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/correlations-monitoring-chart.html',
		scope : {
			clusterid : "=",
			componentid: "=",
            componentname : "=",
			monitoring: "="
		},
		link : function(scope, element, attrs) {
		    scope.timeSeries = {};
		    scope.size = $(element).width();

            scope.dataSource = new MonitoringDataSource(scope.clusterid, scope.componentid, scope.monitoring.name);
            scope.dataSource.listen(function(event){
                scope.addEvent(event);
            }, function(pastData) {
                var options = {
                    type: 'line',
                    series_names: ['correlation'],
                    value_names: ['correlation'],
                    height: scope.size,
                    y_range: [-1.0, 1.0]
                };
                scope.timeSeries.init(options, []);
                $(pastData).each(function(index, event){scope.addEvent(event);});
            });


            scope.features = [];
            scope.correlations = {};
            scope.selectedCorrelation = null;

            scope.addEvent = function(event) {
                var time = Math.round(event.timestamp / 1000);
                scope.updateFeatures(event.data);
                scope.update_correlations_plot(event);
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            scope.updateFeatures =function (data) {
                scope.features = [];
                for (var property in data) {
                    if (data.hasOwnProperty(property)) {
                        var parts = property.split(".");
                        if(!scope.features.contains(parts[0])) {
                            scope.features.push(parts[0]);
                        }
                        if(!scope.features.contains(parts[1])) {
                            scope.features.push(parts[1]);
                        }
                    }
                }
            };

            scope.update_correlations_plot = function (event) {
                var padding = 4;
                var color = d3.scale.linear().domain([-1, 1]).range(["#e74c3c", "#2ecc71"]);
                var formatValue = d3.format(",.2f");
                var cell_size = (scope.size / scope.features.length) - padding;

                for (var combination in event.data) {
                    if (event.data.hasOwnProperty(combination)) {
                        var value = event.data[combination];
                        var parts = combination.split(".");
                        var x = scope.features.indexOfObject(parts[0]) * (cell_size + padding);
                        var y = scope.features.indexOfObject(parts[1]) * (cell_size + padding);

                        var correlation = scope.correlations[combination];
                        if(correlation == null) {
                            correlation = {
                                "name" : parts[0] + "/" + parts[1],
                                "position": "translate(" + x + "," + y + ")",
                                "size": cell_size,
                                "history": FixedQueue(100)
                            };
                            scope.correlations[combination] = correlation;
                        }

                        correlation.value = value;
                        correlation.opacity = Math.abs(value);
                        correlation.color = color(value);
                        correlation.tooltip = correlation.name + " : " + formatValue(value);
                        var correlation_event = {'timestamp': event.timestamp, data: {'correlation': value}};
                        correlation.history.push(correlation_event);
                        if(scope.selectedCombination == combination) {
                            scope.timeSeries.add(correlation_event);
                        }
                    }
                }
            };

            scope.show_correlations_plot = function() {
                $(element).find("#correlation_history").hide();
                $(element).find("#correlations_plot").show();

                scope.selectedCombination = null;
            }

            scope.show_correlation_history = function(combination) {
                $(element).find("#correlations_plot").hide();
                $(element).find("#correlation_history").show();

                scope.selectedCombination = combination;

                scope.timeSeries.clearData();
                scope.timeSeries.title(scope.componentname + " : " + scope.correlations[combination].name);
                scope.timeSeries.addAll(scope.correlations[combination].history);
            };
        }
	};
});
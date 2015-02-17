app.directive('pipelineVisualizationEdition', function(Visualization, VisualizationMetadata) {
    return {
        restrict: "E",
		replace : true,
		templateUrl : 'views/pipeline/pipeline-visualization-edition.html',
        scope : {
            pipeline : "=pipeline",
            visualization : "=visualization",
        },
        link: function (scope, element, attrs) {
            scope.validate = scope.$parent.validate;
        }
    };
});
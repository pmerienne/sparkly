app.directive('visualization', function($compile) {

    return {
        restrict: "E",
        scope : {
            clusterid : "=",
            configuration: "="
        },
        link: function(scope, element, attrs){
            var tag = scope.configuration.metadata.htmlTagName();

            var e = $compile("<" + tag + " clusterid='clusterid' configuration='configuration'></" + tag + ">")(scope);
            element.replaceWith(e);
        }
    };

});
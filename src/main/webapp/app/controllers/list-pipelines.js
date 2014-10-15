app.controller('ListPipelinesCtrl', function ($scope, $location, Pipeline) {
	
	$scope.pipelines = [];
	
	$scope.refreshPipelines = function () {
	    Pipeline.findAll().then(function(pipelines) {
            $scope.pipelines = pipelines;
        });
	};
	

    $scope.deletePipeline = function() {
    	var id = this.pipeline.id;
    	Pipeline.delete(id).then(function() {
            $scope.refreshPipelines();
            console.log("Deleted " + id);
		}, function() {
            console.log("Failing deletion of " + id);
		});
    };
    
    $scope.createNewPipeline = function() {
        var id = Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
        var pipeline = new Pipeline(id, "New pipeline", "", [], []);
        pipeline.save().then(function() {
    		$location.path("pipelines/" + id);
		});
    };
	
	$scope.refreshPipelines();
});
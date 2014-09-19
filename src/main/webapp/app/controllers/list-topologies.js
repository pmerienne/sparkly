app.controller('ListTopologiesCtrl', function ($scope, $location, Topology) {
	
	$scope.topologies = [];
	
	$scope.refreshTopologies = function () {
	    Topology.findAll().then(function(topologies) {
            $scope.topologies = topologies;
        });
	};
	

    $scope.deleteTopology = function() {
    	var id = this.topology.id;
    	Topology.delete(id).then(function() {
            $scope.refreshTopologies();
            console.log("Deleted " + id);
		}, function() {
            console.log("Failing deletion of " + id);
		});
    };
    
    $scope.createNewTopology = function() {
        var id = Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
        var topology = new Topology(id, "New topology", [], []);
        topology.save().then(function() {
    		$location.path("topologies/" + id);
		});
    };
	
	$scope.refreshTopologies();
});
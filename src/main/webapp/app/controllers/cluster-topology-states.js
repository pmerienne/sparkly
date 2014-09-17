app.controller('ClusterTopolyStateCtrl', function($scope, $routeParams, ClusterTopologyStateResource, Topology, NotificationService) {

	$scope.clusterName = $routeParams.clusterName;
	$scope.states = [];
	$scope.topologies = [];
	
	$scope.loadStates = function() {
		ClusterTopologyStateResource.retrieveStates({ clusterName : $scope.clusterName}, function(states) {
			$scope.states = states;
		}, function(error) {
			NotificationService.notify("Unable to states of " + $scope.clusterName, "danger");
		});
	};
	
	$scope.loadTopologies = function() {
		$scope.topologies = Topology.findAll();
	};
	
	$scope.deploy = function(topology) {
		ClusterTopologyStateResource.deploy({ clusterName: $scope.clusterName, topologyId: topology.id}, function() {
			NotificationService.notify(topology.name + " deployed on " + $scope.clusterName);
			$scope.loadStates();
		}, function(error) {
			NotificationService.notify("Unable to deploy " + topology.name + " on " + $scope.clusterName, "danger");
			$scope.loadStates();
		});
	};

	$scope.undeploy = function(topology) {
		ClusterTopologyStateResource.undeploy({ clusterName: $scope.clusterName, topologyId: topology.id}, function() {
			NotificationService.notify(topology.name + " undeploying");
			$scope.loadStates();
		}, function(error) {
			NotificationService.notify("Unable to undeploy " + topology.name + " from " + $scope.clusterName, "danger");
			$scope.loadStates();
		});
	};
	
	$scope.loadStates();
	$scope.loadTopologies();
	
	setInterval(function(){
		$scope.loadStates();
    }, 5000); 
});
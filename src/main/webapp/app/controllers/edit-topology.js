app.controller('EditTopologyCtrl', function($scope, $location, $route, $routeParams, $modal, $timeout,
		NotificationService, JsPlumbService, Topology, Component, ComponentMetadata, ValidationReport, Cluster) {

    $scope.currentView = "Data workflow";

	$scope.topology = {};
	$scope.validationReport = new ValidationReport([]);

	$scope.validate = function() {
	    Topology.validate($scope.topology).then(function(report) {
    	    $scope.validationReport = report;
        }, function(error) {
            NotificationService.notify("Unable to validate topology", "danger");
        });
	};

	$scope.save = function() {
		$scope.topology.save().then(function(data) {
			NotificationService.notify("'" + $scope.topology.name + "' saved");
		}, function(error) {
			NotificationService.notify("'" + $scope.topology.name + "' not saved! ", "danger");
		});
	};

    Topology.findById($routeParams.topologyId).then(function(topology) {
	    $scope.topology = topology;
    }, function(error) {
        NotificationService.notify("Unable to load topology", "danger");
        $location.path("topologies/");
    });

    Cluster.findById("local").then(function(cluster) {
        $scope.cluster = cluster;
    });

    $scope.launch = function(clusterId) {
        Cluster.findById("local").then(function(cluster) {
            NotificationService.notify("Deploying topology on " + cluster.name);
            cluster.deploy($scope.topology.id).then(function(success) {
                NotificationService.notify("Topology " + $scope.topology.name + " deployed on " + cluster.name);
            }, function(error) {
                NotificationService.notify("Unable to deploy topology " + $scope.topology.name + " on " + cluster.name, "danger");
            });
        });
    };
});
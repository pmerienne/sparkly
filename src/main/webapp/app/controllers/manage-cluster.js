app.controller('ManageClusterCtrl', function($scope, $route, $location, $routeParams, $timeout,
    NotificationService, Cluster, Topology) {

	$scope.cluster = {};

    Cluster.findById($routeParams.clusterId).then(function(cluster) {
        $scope.cluster = cluster;
        $scope.updateStatus();
    }, function(error) {
        NotificationService.notify("Unable to load cluster " + $routeParams.clusterId, "danger");
        $location.path("/");
    });

	$scope.topologies = [];
    Topology.findAll().then(function(topologies) {
        $scope.topologies = topologies;
    });

    $scope.deploy = function(topology) {
        $scope.cluster.deploy(topology.id).then(function(success) {
            NotificationService.notify("Topology " + topology.name + " deployed on " + $scope.cluster.name);
         }, function(error) {
            NotificationService.notify("Unable to deploy topology " + topology.name + " on " + $scope.cluster.name, "danger");
         });
    };

    $scope.updateStatus = function() {
        $scope.cluster.updateStatus();
        $scope.updateStateCss();
        $timeout($scope.updateStatus, 1000);
    }

    $scope.stateCss = "warning";
    $scope.updateStateCss = function() {
        switch($scope.cluster.status.state) {
            case "Running":
                $scope.stateCss = "success";
                break;
            case "Stopped":
                $scope.stateCss = "danger";
                break;
            default :
                $scope.stateCss = "warning"
                break;
        }
    }
});
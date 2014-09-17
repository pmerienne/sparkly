app.controller('EditComponentCtrl', function($scope, $modalInstance, component, topology) {

	$scope.component = component;
	$scope.topology = topology;

	$scope.saveComponent = function() {
		$modalInstance.close($scope.component);
	};

	$scope.deleteComponent = function() {
		$modalInstance.close(null);
	};

});
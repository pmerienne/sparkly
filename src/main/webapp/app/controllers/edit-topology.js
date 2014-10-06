app.controller('EditTopologyCtrl', function($scope, $location, $route, $routeParams, $modal, $timeout,
		NotificationService, JsPlumbService, Topology, Component, ComponentMetadata, ValidationReport, Cluster) {

	var REDRAW_TIMEOUT = 50;

	$scope.topology = {};
	$scope.validationReport = new ValidationReport([]);

	$scope.validate = function() {
	    Topology.validate($scope.topology).then(function(report) {
    	    $scope.validationReport = report;
        }, function(error) {
            NotificationService.notify("Unable to validate topology", "danger");
        });
	};

	$scope.displayTopology = function(topology) {
		$scope.topology = topology;

		$scope.diagram = JsPlumbService.createDiagram("jsplumb-container");
		setTimeout(function() {
			$scope.diagram.bindTopologyConnections($scope.topology);
			$scope.redraw();
		}, REDRAW_TIMEOUT);

		$scope.validate();
	};

    Topology.findById($routeParams.topologyId).then(function(topology) {
	    $scope.displayTopology(topology);
    }, function(error) {
        NotificationService.notify("Unable to load topology", "danger");
        $location.path("topologies/");
    });

	$scope.editComponent = function(component) {
		var modalInstance = $modal.open({
			templateUrl : 'views/component/edit-component-modal.html',
			controller : 'EditComponentCtrl',
			resolve : {
				component : function() {
					return component;
				}, topology: function() {
					return $scope.topology;
				}
			}
		});

		modalInstance.result.then(function(editedComponent) {
			if (!editedComponent) {
				$scope.removeComponent(component);
			} else {
				console.log("'" + component.name + "' saved");
			}
			$scope.validate();
		}, function(component) {
			// Edition canceled
		});
	};

	$scope.removeComponent = function(component) {
		$scope.diagram.clearComponent(component);
		$scope.topology.components.remove(component);
	};

	$scope.save = function() {
		$scope.topology.save().then(function(data) {
			NotificationService.notify("'" + $scope.topology.name + "' saved");
		}, function(error) {
			NotificationService.notify("'" + $scope.topology.name + "' not saved! ", "danger");
		});
	};

	$scope.remove = function() {
		Topology.delete($scope.topology.id).then(function() {
			NotificationService.notify("Topology deleted");
			$location.path("topologies/");
		});
	};

	$scope.revert = function() {
		$route.reload();
	};

	$scope.addNewComponent = function(metadata, x, y) {
		var component = Component.newComponent(metadata);
		if(x) component.x = x;
		if(y) component.y = y;
		$scope.topology.components.push(component);
		$scope.redraw(true);
		$scope.validate();
	};

	$scope.redraw = function(deffered) {
		if (!deffered) {
			$scope.diagram.jsPlumbInstance.repaintEverything();
		} else {
			setTimeout(function() {
				$scope.diagram.jsPlumbInstance.repaintEverything();
			}, REDRAW_TIMEOUT);
		}
	};

	$scope.logEvent = function(event) {
	    console.log("Yippi !");
	    console.log(event);
	};

	ComponentMetadata.findAll().then(function(metadatas) {
	    $scope.metadataByCategories = {};
	    $scope.metadataByIds = {};
	    for(var id in metadatas) {
            if (metadatas.hasOwnProperty(id)) {
                var metadata = metadatas[id];
                if(!$scope.metadataByCategories.hasOwnProperty(metadata.category)) {
                    $scope.metadataByCategories[metadata.category] = [];
                }
                $scope.metadataByCategories[metadata.category].push(metadata);
                $scope.metadataByIds[metadata.id] = metadata;
                $scope.makeDraggable(metadata.id.toId());
            }
	    }
	});

	$("#add-component-panel").bind("dragover", function (e) {
        e.preventDefault();
        return false;
    });

	$("#add-component-panel").bind("dragenter", function dragEnter(e) {
        e.preventDefault();
        return true;
    });

	$("#add-component-panel").bind("drop", function (e) {
        var x = e.originalEvent.offsetX;
        var y = e.originalEvent.offsetY;
        var id = e.originalEvent.dataTransfer.getData("metadata-id");
        var metadata = $scope.metadataByIds[id];
        $scope.addNewComponent(metadata, x, y);
    });

	$scope.makeDraggable = function(elementId) {
	    $timeout(function(){
	        var element = $("#" + elementId);
            element.attr('draggable', 'true');
            element.attr('style', 'cursor:move;');
            element.bind("dragstart", function(event){
                event.originalEvent.dataTransfer.setData("metadata-id", element.attr("metadata-id"));
                $("#add-component-panel").removeClass("ng-hide");
                $("#jsplumb-panel").addClass("glow");
            });
            element.bind("dragend", function(event){
                $("#add-component-panel").addClass("ng-hide");
                $("#jsplumb-panel").removeClass("glow");
            });
        }, 500);
	};

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
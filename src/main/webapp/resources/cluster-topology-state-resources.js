app.factory('ClusterTopologyStateResource', function($resource) {
	return $resource('api/clusters/:clusterName/topologies/:topologyId', {
		clusterName : '@clusterName',
		topologyId : '@topologyId'
	}, {
		deploy : {method: 'POST'},
		undeploy : {method: 'DELETE'},
		retrieveState : {method: 'GET'}, 
		retrieveStates : {method: 'GET', isArray: true}, 
	});
});
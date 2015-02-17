app.factory('VisualizationMetadata', function($http, PropertyMetadata) {

    function VisualizationMetadata(id, name, properties, streams, features) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.streams = streams;
        this.features = features;
    };

    VisualizationMetadata.prototype.property = function(name) {
        return $.grep(this.properties, function (property) { return property.name == name })[0]
    };

    VisualizationMetadata.build = function (data) {
        return new VisualizationMetadata(
            data.id,
            data.name,
            data.properties.map(PropertyMetadata.build),
            data.streams,
            data.features
        );
    };

    VisualizationMetadata.prototype.htmlTagName = function() {
        return this.id.replace(/\./g, "-");
    };

    VisualizationMetadata.findAll = function() {
        return $http.get('api/visualizations/').then(function(visualizationsData) {
            return $.map(visualizationsData.data, VisualizationMetadata.build);
        });
    };

    return VisualizationMetadata;
});

app.factory('Visualization', function($http, VisualizationMetadata, Property, PropertyMetadata) {

    function Visualization(id, name, metadata, properties, streams, features) {
        this.id = id;
        this.name = name;
        this.metadata = metadata;
        this.properties = properties;
        this.streams = streams;
        this.features = features;
    };

    Visualization.build = function (data) {
        var metadata = VisualizationMetadata.build(data.metadata);

        var streams = metadata.streams.map(function(stream) {
            var streamData = data.streams.tryFind(function(s){return s.name == stream});
            return streamData == null ? {name: stream, component: "", stream: ""} : streamData;
        });

        var features = metadata.features.map(function(feature) {
            var featureData = data.features.tryFind(function(f){return f.name == feature});
            return featureData == null ? {name: stream, component: "", stream: "", feature: undefined} : featureData;
        });

        var properties = metadata.properties.map(function(propertyMetadata) {
            var propertyData = data.properties.tryFind(function(property){return property.name == propertyMetadata.name});
            return propertyData == null ? Property.newProperty(propertyMetadata) : Property.build(propertyMetadata, propertyData);
        });

        return new Visualization(data.id, data.name, metadata, properties, streams, features);
    };

    Visualization.newVisualization = function(metadata) {
        var id = Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
        var name = metadata.name;
        var properties = metadata.properties.map(Property.newProperty)
        var streams = metadata.streams.map(function(stream) {return {name: stream, component: "", stream: ""}});
        var features = metadata.features.map(function(feature) {return {name: feature, component: "", stream: "", feature: ""}});

        return new Visualization(id, name, metadata, properties, streams, features);
    };

    return Visualization;
});

app.factory('VisualizationDataSource', function($http) {

    function VisualizationDataSource(master, id) {
        this.master = master;
        this.id = id;
        this.sockets = [];
    }

    VisualizationDataSource.prototype.listen = function(onData, onPastData) {
        var self = this;
        var socket = $.atmosphere;
        this.sockets.push(socket);
        var request = {
            url: 'api/visualizations/data/' + this.master + '/' + this.id,
            logLevel: 'info',
            transport: 'websocket'
        };
        request.onMessage = function(message) {
            var json = jQuery.parseJSON(message.responseBody);
            onData(json);
        };
        request.onOpen = function(message) {
            request.transport = message.transport;
        };

        if(onPastData) {
            $http.get('api/visualizations/past_data/' + this.master + '/' + this.id).then(function(pastData) {
                onPastData(pastData.data);
                socket.subscribe(request);
            });
        } else {
            socket.subscribe(request);
        }
    }


    VisualizationDataSource.prototype.close = function() {
        $.each(this.sockets, function(index, socket) {
            socket.unsubscribe();
        });
    }

    return VisualizationDataSource;
});
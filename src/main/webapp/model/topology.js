app.factory('Topology', function($http, Component, Connection, ValidationReport) {

    function Topology(id, name, description, components, connections) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.components = components;
        this.connections = connections;
    };

    Topology.build = function (data) {
        return new Topology(
            data.id,
            data.name,
            data.description,
            data.components.map(Component.build),
            data.connections.map(Connection.build)
        );
    };

    Topology.findAll = function() {
        return $http.get('api/topologies/').then(function(topologies) {
            return $.map(topologies.data, Topology.build);
        });
    };

    Topology.findById = function(id) {
        return $http.get('api/topologies/' + id).then(function(topology) {
            return Topology.build(topology.data);
        });
    };

    Topology.delete = function(id) {
        return $http.delete('api/topologies/' + id);
    };

    Topology.prototype.save = function() {
        return $http.put('api/topologies/' , this);
    };

    Topology.validate = function(topology) {
        return $http.post('api/pipeline-validation', topology).then(function(report) {
            return ValidationReport.build(report.data);
        });
    };

    Topology.prototype.retrieveAvailableFeatures = function(component, stream) {
        var self = this;
        var features = [];
        var connections = this.connectedTo(component.id, stream.name);

        connections.forEach(function(connection) {
            var otherComponent = self.component(connection.from.component);
            var os = otherComponent.outputStream(connection.from.stream);

            var osFeatures = os.availableFeatures();
            features.pushAll(osFeatures);

            if(os.metadata.from) {
                var is = otherComponent.inputStream(os.metadata.from)
                features.pushAll(self.retrieveAvailableFeatures(otherComponent, is));
            }
        });

        return features;
    };

    Topology.prototype.connectedTo = function(componentId, streamName) {
        var self = this;
        var connectionsTo = [];

        this.connections.forEach(function (connection) {
            if(connection.to.component == componentId && connection.to.stream == streamName) {
                connectionsTo.push(connection);
            }
        });

        return connectionsTo;
    };

    Topology.prototype.removeConnectionsOf = function(componentId) {
        this.connections = $.grep(this.connections, function (connection) {
            return connection.to.component != componentId && connection.from.component != componentId;
        });
    };

    Topology.prototype.component = function(componentId) {
        return $.grep(this.components, function (component) { return component.id == componentId})[0];
    };

    return Topology;
});

app.factory('ValidationReport', function(ValidationMessage) {

    function ValidationReport(messages) {
        this.messages = messages;
        this.errorCount = $.grep(this.messages, function (message) { return message.level == "Error"}).length;
        this.warningCount = $.grep(this.messages, function (message) { return message.level == "Warning"}).length;
    }

    ValidationReport.build = function (data) {
        var messages = data.messages.map(ValidationMessage.build)
        return new ValidationReport(messages);
    };

    ValidationReport.prototype.isEmpty = function() {
        return this.messages.length <= 0;
    };

    ValidationReport.prototype.size = function() {
        return this.messages.length;
    };

    return ValidationReport;
});


app.factory('ValidationMessage', function() {

    function ValidationMessage(level, text) {
        this.level = level;
        this.text = text;
    }

    ValidationMessage.build = function (data) {
        return new ValidationMessage(data.level, data.text);
    };

    return ValidationMessage;
});

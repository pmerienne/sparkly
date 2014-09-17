app.factory('ComponentMetadata', function($http, PropertyMetadata, StreamMetadata) {

    function ComponentMetadata(name, description, properties, inputStreams, outputStreams) {
        this.name = name;
        this.description = description;
        this.properties = properties;
        this.inputStreams = inputStreams;
        this.outputStreams = outputStreams;
    };

    ComponentMetadata.prototype.property = function(name) {
        return $.grep(this.properties, function (property) { return property.name == name })[0]
    };

    ComponentMetadata.prototype.inputStream = function(name) {
        return $.grep(this.inputStreams, function (inputStream) { return inputStream.name == name })[0]
    };

    ComponentMetadata.prototype.outputStream = function(name) {
        return $.grep(this.outputStreams, function (outputStream) { return outputStream.name == name })[0]
    };

    ComponentMetadata.build = function (data) {
        return new ComponentMetadata(
            data.name,
            data.description,
            data.properties.map(PropertyMetadata.build),
            data.inputStreams.map(StreamMetadata.build),
            data.outputStreams.map(StreamMetadata.build)
        );
    };

    ComponentMetadata.findAll = function() {
        return $http.get('api/components/').then(function(componentsData) {
            return $.map(componentsData.data, ComponentMetadata.build);
        });
    };

    return ComponentMetadata;
});

app.factory('Component', function($http, ComponentMetadata, Property, Stream) {

	function Component(metadata, id, name, inputStreams, outputStreams, properties, x, y) {
		this.metadata = metadata;

		this.id = id;
		this.name = name;

		this.inputStreams = inputStreams;
		this.outputStreams = outputStreams;
		this.properties = properties;

		this.x = x;
		this.y = y;
	};

	Component.build = function (data) {
	    var metadata = ComponentMetadata.build(data.metadata);

        var outputStreams = data.outputStreams.map(function(streamData) {
            var streamMetadata = metadata.outputStream(streamData.name);
            return Stream.build(streamMetadata, streamData);
        });

        var inputStreams = data.inputStreams.map(function(streamData) {
            var streamMetadata = metadata.inputStream(streamData.name);
            return Stream.build(streamMetadata, streamData);
        });

	    var properties = data.properties.map(function(propertyData) {
	        var propertyMetadata = metadata.property(propertyData.name);
	        return Property.build(propertyMetadata, propertyData);
	    });

        return new Component(metadata, data.id, data.name, inputStreams, outputStreams, properties, data.x, data.y);
    };

    Component.newComponent = function(metadata) {
        var id = Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
        var name = metadata.name;
        var inputStreams = metadata.inputStreams.map(Stream.newStream)
        var outputStreams = metadata.outputStreams.map(Stream.newStream)
        var properties = metadata.properties.map(Property.newProperty)

        return new Component(metadata, id, name, inputStreams, outputStreams, properties, 50, 50);
    };

    Component.prototype.hasInputs = function () {
        return this.inputStreams.length > 0;
    };

    Component.prototype.hasOutputs = function () {
        return this.outputStreams.length > 0;
    };

    Component.prototype.hasProperties = function () {
        return this.properties.length > 0;
    };

    Component.prototype.inputStream = function(name) {
        return $.grep(this.inputStreams, function (inputStream) { return inputStream.name == name })[0]
    };

    Component.prototype.outputStream = function(name) {
        return $.grep(this.outputStreams, function (outputStream) { return outputStream.name == name })[0]
    };

    return Component;
})

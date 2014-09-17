app.factory('StreamMetadata', function() {

    function StreamMetadata(name, from, listedFeatures, namedFeatures) {
        this.name = name;
        this.from = from;
        this.listedFeatures = listedFeatures;
        this.namedFeatures = namedFeatures;
    }

    StreamMetadata.build = function (data) {
        return new StreamMetadata (
          data.name,
          data.from,
          data.listedFeatures,
          data.namedFeatures
        );
    };

    return StreamMetadata;
});

app.factory('Stream', function(StreamMetadata) {

    function Stream(metadata, name, selectedFeatures, mappedFeatures) {
        this.metadata = metadata;
        this.name = name;
        this.selectedFeatures = selectedFeatures;
        this.mappedFeatures = mappedFeatures;
    }

    Stream.build = function (metadata, data) {
        return new Stream(metadata, data.name, data.selectedFeatures, data.mappedFeatures);
    };

    Stream.newStream = function (metadata) {
        var selectedFeatures = {}
        for(var i = 0; i < metadata.listedFeatures.length; i++) {
            selectedFeatures[metadata.listedFeatures[i]] = [];
        }

        var mappedFeatures = {};
        for(var i = 0; i < metadata.namedFeatures.length; i++) {
            mappedFeatures[metadata.namedFeatures[i]] = "New feature : " + metadata.namedFeatures[i];
        }

        return new Stream(metadata, metadata.name, selectedFeatures, mappedFeatures);
    };

    Stream.prototype.availableFeatures = function() {
        var features = [];
        $.each(this.mappedFeatures, function(name, value) {
            features.push(value);
        });

        $.each(this.selectedFeatures, function(name, values) {
           $.each(values, function(index, value) {
            features.push(value);
           });
        });

        return features;
    };

    return Stream;
});
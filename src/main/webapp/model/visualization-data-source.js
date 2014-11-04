app.factory('VisualizationDataSource', function() {

    function VisualizationDataSource(master, id) {
        this.master = master.replace(/[^a-z0-9]/gmi, "");
        this.id = id.replace(/[^a-z0-9]/gmi, "")
    }

    VisualizationDataSource.prototype.listen = function(onData) {
        var socket = $.atmosphere
        var request = {
            url: 'api/visualization/' + this.master + '/' + this.id,
            transport: 'websocket'
        };
        request.onMessage = function(message) {
            var json = jQuery.parseJSON(message.responseBody);
            onData(json);
        };
        request.onOpen = function(message) {
            request.transport = message.transport;
        };

        socket.subscribe(request);
    }

    return VisualizationDataSource;
});
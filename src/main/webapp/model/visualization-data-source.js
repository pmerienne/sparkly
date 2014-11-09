app.factory('VisualizationDataSource', function() {

    function VisualizationDataSource(master, id) {
        this.master = master;
        this.id = id;
        this.sockets = [];
    }

    VisualizationDataSource.prototype.listen = function(onData) {
        var socket = $.atmosphere;
        this.sockets.push(socket);
        var request = {
            url: 'api/visualization/' + this.master + '/' + this.id,
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
        socket.subscribe(request);
    }

    VisualizationDataSource.prototype.close = function() {
        $.each(this.sockets, function(index, socket) {
            socket.unsubscribe();
        });
    }

    return VisualizationDataSource;
});
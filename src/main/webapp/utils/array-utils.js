Array.prototype.indexOfObject = function(obj) {
	for ( var i = 0, len = this.length; i < len; i++) {
		if (angular.equals(this[i], obj))
			return i;
	}
	return -1;
};

Array.prototype.remove = function(obj) {
	var index = this.indexOfObject(obj);

	if (index > -1) {
		this.splice(index, 1);
	}
};

Array.prototype.pushAll = function() {
    for (var a = 0;  a < arguments.length;  a++) {
        arr = arguments[a];
        for (var i = 0;  i < arr.length;  i++) {
            this.push(arr[i]);
        }
    }
}

Array.prototype.tryFind = function(predicate) {
    var values = $.grep(this, predicate );
    return values.isEmpty() ? null : values[0];
}

Array.prototype.isEmpty = function() {
    return !this.length > 0;
}
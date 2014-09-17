app.directive('switch', function() {

	return {
		restrict : "A",
		link : function(scope, element, attrs) {
			element.attr("data-on-color", "primary");
			element.attr("data-off-color", "default");
			element.attr("data-animate", "false");
			// element.attr("data-on-text", "<i class='fa fa-check'></i>");
			// element.attr("data-off-text", "<i class='fa fa-times'></i>");
			element.attr("data-on-text", "ON");
			element.attr("data-off-text", "OFF");
			element.attr("checked", scope.property.value);

			$(element).bootstrapSwitch();
			$(element).on('switchChange', function(e, data) {
				scope.property.value = data.value.toString();
			});
		}
	};
});

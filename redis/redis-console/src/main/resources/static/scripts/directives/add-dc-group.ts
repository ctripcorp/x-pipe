/**  确认框 */
angular
    .module('directive')
    .directive('adddcgroup', adddcgroup);

function adddcgroup($compile, $window) {
    return {
        restrict: 'E',
        templateUrl: '../../views/directives/add-dc-group.html',
        transclude: true,
        replace: true
    }
}
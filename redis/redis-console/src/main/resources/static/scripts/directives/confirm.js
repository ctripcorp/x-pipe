/**  确认框 */
directive_module.directive('xpipeconfirmdialog', function ($compile, $window) {
    return {
        restrict: 'E',
        templateUrl: '../../views/directives/confirm-dialog.html',
        transclude: true,
        replace: true,
        scope: {
            dialogId: '=xpipeDialogId',
            title: '=xpipeTitle',
            detail: '=xpipeDetail',
            showCancelBtn: '=xpipeShowCancelBtn',
            doConfirm: '=xpipeConfirm',
            doCancel: '=xpipeCancel'
        },
        link: function (scope, element, attrs) {

            scope.confirm = function () {
                if (scope.doConfirm){
                    scope.doConfirm();
                }
            };
            scope.cancel = function () {
                if (scope.doCancel) {
                    scope.doCancel();
                }
            };

        }
    }
});

angular
    .module('utils')
    .service('AppUtil', AppUtil);

AppUtil.$inject = ['toastr', '$window'];

function AppUtil(toastr, $window) {

    return {
        errorMsg: function (response) {
            if (response.status == -1) {
                return "您的登录信息已过期,请刷新页面后重试";
            }
            var msg = "Code:" + response.status;
            if (response.data.message != null) {
                msg += " Msg:" + response.data.message;
            }
            return msg;
        },
        parseParams: function (query, notJumpToHomePage) {
            if (!query) {
                //如果不传这个参数或者false则返回到首页(参数出错)
                if (!notJumpToHomePage) {
                    $window.location.href = '/index.html';
                } else {
                    return {};
                }
            }
            if (query.indexOf('/') == 0) {
                query = query.substring(1, query.length);
            }
            var params = query.split("&");
            var result = {};
            params.forEach(function (param) {
                var kv = param.split("=");
                result[kv[0]] = kv[1];
            });
            return result;
        }
    }
}

angular
    .module('cluster_type')
    .service('ClusterType', ClusterType);

function ClusterType() {
    return {
        _values: {
            'one_way': {
                name: '单向同步',
                value: 'one_way',
                multiActiveDcs: false,
                useKeeper: true,
                healthCheck: true,
            },
            'bi_direction': {
                name: '双向同步',
                value: 'bi_direction',
                multiActiveDcs: true,
                useKeeper: false,
                healthCheck: true,
            },
            'single_dc': {
                name: '单机房缓存',
                value: 'single_dc',
                multiActiveDcs: false,
                useKeeper: false,
                healthCheck: false,
            },
            'local_dc': {
                name: 'ben机房缓存',
                value: 'local_dc',
                multiActiveDcs: true,
                useKeeper: false,
                healthCheck: false,
            }
        },
        lookup(typeName) {
            return (typeName && this._values[typeName.toLowerCase()]) || null
        },
        values() {
            return Object.values(this._values)
        },
        selectData() {
            var data = [{id: "", title: ""}];
            for (var key in this._values) {
                data.push({
                    id: this._values[key].value,
                    title: this._values[key].name
                });
            }

            return data;
        },
        default() {
            return this._values.one_way
        }
    }
}
cluster_type.service('ClusterType', [function () {
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
    }]
)
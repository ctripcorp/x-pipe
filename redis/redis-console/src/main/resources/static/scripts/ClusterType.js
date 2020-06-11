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
                    healthCheck: false,
                }
            },
            lookup(typeName) {
                return (typeName && this._values[typeName.toLowerCase()]) || null
            },
            values() {
                return Object.values(this._values)
            },
            default() {
                return this._values.one_way
            }
        }
    }]
)
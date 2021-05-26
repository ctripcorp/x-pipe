import { Component } from '@angular/core';

@Component({
    selector: 'cluster-list',
    template: `<ng2-smart-table [settings]="settings" [source]="source" 
                                (userRowSelect)="onUserRowSelect($event)"></ng2-smart-table>`
})

export class ClusterListComponent {
    settings = {
        actions: false,
        selectMode: 'multi',
        columns: {
            cluster: {
                title: 'Cluster'
            },
            activeidc: {
                title: '主机房'
            },
            clustertype: {
                title: '集群类型'
            },
            description: {
                title: '描述'
            },
            organization: {
                title: '组织'
            },
            email: {
                title: '联系人邮箱'
            }
        }
    };

    source = [
        {
            cluster: "cluster_2",
            activeidc: "SHAJQ",
            clustertype: "ONE-WAY",
            description: "test cluster"
        }
    ];

    private selectedRows: any;

    onUserRowSelect(event) {
        this.selectedRows = event.selected;
        console.log(this.selectedRows);
        console.log(event.selected);
    }
}

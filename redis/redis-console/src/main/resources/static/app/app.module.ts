import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { UpgradeModule, setAngularJSGlobal, downgradeComponent } from '@angular/upgrade/static';

import * as angular from 'angular'; setAngularJSGlobal(angular);
import 'zone.js/dist/zone';
import 'core-js'

import { UIRouterUpgradeModule } from '@uirouter/angular-hybrid';
import { UIRouterModule } from '@uirouter/angular';

import '../scripts/app.angularjs.module';

import { Ng2SmartTableModule } from 'ng2-smart-table';

import { ClusterListComponent } from "./cluster.list.component";

angular
    .module('directive')
    .directive(
        'clusterList',
        downgradeComponent({ component: ClusterListComponent }) as angular.IDirectiveFactory
    );

@NgModule({
    imports: [
        BrowserModule,
        UpgradeModule,
        UIRouterUpgradeModule,
        Ng2SmartTableModule,
    ],
    declarations: [
        ClusterListComponent
    ],
    entryComponents: [
        ClusterListComponent
    ]
})
export class AppModule {
    constructor(private upgrade: UpgradeModule) { }

    ngDoBootstrap() {
        this.upgrade.bootstrap(document.body, ["index"], { strictDi: false });
    }
}

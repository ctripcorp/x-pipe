import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {BrowserAnimationsModule} from "@angular/platform-browser/animations";
import {UpgradeModule, setAngularJSGlobal, downgradeComponent} from '@angular/upgrade/static';

import * as angular from 'angular'; setAngularJSGlobal(angular);
import 'zone.js/dist/zone';
import 'core-js'

import {UIRouterUpgradeModule} from '@uirouter/angular-hybrid';
import {UIRouterModule} from '@uirouter/angular';

import '../scripts/app.angularjs.module';

import {Ng2SmartTableModule} from 'ng2-smart-table';
import {MatSelectModule} from '@angular/material/select';

import {DynamicStringValueSelectComponent} from "./dynamic-string-value-select.component";

angular
    .module('directive')
    .directive(
        'dynamicStringValueSelect',
        downgradeComponent({ component: DynamicStringValueSelectComponent }) as angular.IDirectiveFactory
    );

@NgModule({
    imports: [
        BrowserModule,
        BrowserAnimationsModule,
        UpgradeModule,
        UIRouterUpgradeModule,
        Ng2SmartTableModule,
        MatSelectModule
    ],
    declarations: [
        DynamicStringValueSelectComponent
    ],
    entryComponents: [
        DynamicStringValueSelectComponent
    ]
})
export class AppModule {
    constructor(private upgrade: UpgradeModule) { }

    ngDoBootstrap() {
        this.upgrade.bootstrap(document.body, ["index"], { strictDi: false });
    }
}

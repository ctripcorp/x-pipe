import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { UpgradeModule, setAngularJSGlobal } from '@angular/upgrade/static';

import * as angular from 'angular'; setAngularJSGlobal(angular);
import 'zone.js/dist/zone';
import 'core-js'

import '../scripts/app';

@NgModule({
    imports: [ BrowserModule, UpgradeModule ]
})

export class AppModule {
    constructor(private upgrade: UpgradeModule) { }

    ngDoBootstrap() {
        this.upgrade.bootstrap(document.body, ["index"], { strictDi: false });
    }
}

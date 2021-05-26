import { Component, Input, OnInit } from '@angular/core';

@Component({
    template: `<button (click)="example()">Click me</button>`,
})
export class ButtonRenderComponent implements OnInit {

    public renderValue;

    @Input() value;

    constructor() {  }

    ngOnInit() {
        this.renderValue = this.value;
    }

    example() {
        alert(this.renderValue);
    }
}
import {Component, EventEmitter, Input, OnInit, Output} from '@angular/core';

@Component({
    selector: 'dynamic-string-value-select',
    templateUrl: `app/dynamic-string-value-select.component.html`
})

export class DynamicStringValueSelectComponent implements OnInit {
    @Input() title?;
    @Input() values?;

    @Input() value;
    @Output() valueChange: EventEmitter<String> = new EventEmitter<String>();

    @Output() selected: EventEmitter<String> = new EventEmitter<String>();

    ngOnInit() {
        console.log("DynamicStringValueSelectComponent: ngOnInit", this.values);
    }

    onClusterTypeChange(event) {
        console.log("DynamicStringValueSelectComponent: selected", event);
        this.valueChange.emit(event.target.value);
        this.selected.emit(event.target.value);
    }
}
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NbButtonModule, NbCardModule, NbCheckboxModule } from '@nebular/theme';
import { TranslateModule } from '@ngx-translate/core';

import { RealtimeSha256Component } from './realtime-sha256.component';

@NgModule({
  declarations: [
    RealtimeSha256Component
  ],
  imports: [
    CommonModule,
    NbCardModule,
    NbButtonModule,
    NbCheckboxModule,
    TranslateModule,
  ],
  exports: [
    RealtimeSha256Component
  ]
})
export class RealtimeSha256Module { }

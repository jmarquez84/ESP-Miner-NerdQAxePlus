import { ChangeDetectionStrategy, ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { bufferTime, filter as rxFilter } from 'rxjs/operators';

import { WebsocketService, WS_SHARES } from '../../services/web-socket.service';

/** What the pool said about a nonce we sent it. */
export type ShareState =
  | 'local'     // below the pool difficulty, it never left the device
  | 'pending'   // submitted, still waiting for the pool
  | 'accepted'
  | 'rejected'
  | 'orphan';   // the pool reconnected before answering, the id is gone

export interface ShareRow {
  key: number;
  ts: number;
  pool: number;
  hash: string;
  zeros: number;
  diff: number;
  poolDiff: number;
  asicDiff: number;
  nbits: number;
  ntime: number;
  nonce: number;
  version: number;
  job: string;
  en2: string;
  asic: number;
  dup: boolean;
  sid: number;
  state: ShareState;
  reason?: string;
}

/** Rows kept in the DOM. The stream is slow, this is minutes of history. */
const MAX_ROWS = 200;

@Component({
  selector: 'app-realtime-sha256',
  templateUrl: './realtime-sha256.component.html',
  styleUrls: ['./realtime-sha256.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RealtimeSha256Component implements OnInit, OnDestroy {

  public rows: ShareRow[] = [];
  public connected = false;

  public accepted = 0;
  public rejected = 0;
  public pending = 0;
  public verified = 0;
  public bestDiff = 0;
  public dropped = 0;

  /** only nonces that reached the pool */
  public onlySubmitted = false;
  public paused = false;

  private subscription?: Subscription;
  private nextKey = 1;

  /** `${pool}:${sid}` -> row, for the shares still waiting on a verdict */
  private waiting = new Map<string, ShareRow>();

  constructor(
    private websocketService: WebsocketService,
    private ngZone: NgZone,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    // The device pushes a coalesced frame every 100 ms at most, but keep the
    // parsing out of Angular and re-enter once per batch anyway.
    this.ngZone.runOutsideAngular(() => {
      this.subscription = this.websocketService
        .connect(WS_SHARES)
        .pipe(
          bufferTime(100),
          rxFilter((batch) => batch.length > 0),
        )
        .subscribe({
          next: (batch) => {
            const events = this.parse(batch);
            if (!events.length) {
              return;
            }
            this.ngZone.run(() => this.apply(events));
          },
          error: () => this.ngZone.run(() => {
            this.connected = false;
            this.cdr.markForCheck();
          }),
          complete: () => this.ngZone.run(() => {
            this.connected = false;
            this.cdr.markForCheck();
          }),
        });
    });

    this.connected = true;
  }

  ngOnDestroy(): void {
    this.subscription?.unsubscribe();
    this.websocketService.close(WS_SHARES);
  }

  public trackByKey(_index: number, row: ShareRow): number {
    return row.key;
  }

  public clear(): void {
    this.rows = [];
    this.waiting.clear();
    this.accepted = 0;
    this.rejected = 0;
    this.pending = 0;
    this.verified = 0;
    this.bestDiff = 0;
    this.dropped = 0;
    this.cdr.markForCheck();
  }

  public togglePause(): void {
    this.paused = !this.paused;
    this.cdr.markForCheck();
  }

  public toggleOnlySubmitted(): void {
    this.onlySubmitted = !this.onlySubmitted;
    this.cdr.markForCheck();
  }

  public get visibleRows(): ShareRow[] {
    return this.onlySubmitted ? this.rows.filter((r) => r.state !== 'local') : this.rows;
  }

  public hex(value: number, width: number = 8): string {
    // the device sends these as unsigned 32 bit numbers
    return (value >>> 0).toString(16).padStart(width, '0');
  }

  /** leading zeros of the hash, the part everybody actually looks at */
  public zerosOf(row: ShareRow): string {
    return row.hash.slice(0, row.zeros);
  }

  public restOf(row: ShareRow): string {
    return row.hash.slice(row.zeros);
  }

  /**
   * The device clock only becomes a real epoch once SNTP has synced; before
   * that now_ms() counts from boot. Fall back to the browser's clock so the
   * column never shows 1970.
   */
  private timestamp(ts: number | undefined): number {
    return ts && ts > 1609459200000 ? ts : Date.now();
  }

  private parse(batch: string[]): any[] {
    const out: any[] = [];
    for (const frame of batch) {
      try {
        const parsed = JSON.parse(String(frame));
        if (Array.isArray(parsed)) {
          out.push(...parsed);
        }
      } catch {
        // a malformed frame is not worth tearing the view down for
      }
    }
    return out;
  }

  private apply(events: any[]): void {
    this.connected = true;

    for (const ev of events) {
      switch (ev.t) {
        case 'n':
          this.onNonce(ev);
          break;
        case 'v':
          this.onVerdict(ev);
          break;
        case 'r':
          this.onReset(ev);
          break;
        case 'd':
          this.dropped += ev.n ?? 0;
          break;
        default:
          break;
      }
    }

    if (this.rows.length > MAX_ROWS) {
      // rows that fall off the end can still be waiting; drop them from the
      // correlation map too or it grows without bound
      for (const row of this.rows.slice(MAX_ROWS)) {
        if (row.state === 'pending') {
          this.waiting.delete(`${row.pool}:${row.sid}`);
          this.pending--;
        }
      }
      this.rows = this.rows.slice(0, MAX_ROWS);
    }

    this.cdr.markForCheck();
  }

  private onNonce(ev: any): void {
    const hash: string = ev.h ?? '';
    let zeros = 0;
    while (zeros < hash.length && hash[zeros] === '0') {
      zeros++;
    }

    const sid: number = ev.sid ?? -1;

    const row: ShareRow = {
      key: this.nextKey++,
      ts: this.timestamp(ev.ts),
      pool: ev.p ?? 0,
      hash,
      zeros,
      diff: ev.d ?? 0,
      poolDiff: ev.pd ?? 0,
      asicDiff: ev.ad ?? 0,
      nbits: ev.nb ?? 0,
      ntime: ev.nt ?? 0,
      nonce: ev.no ?? 0,
      version: ev.v ?? 0,
      job: ev.job ?? '',
      en2: ev.en2 ?? '',
      asic: ev.a ?? -1,
      dup: ev.dup === true,
      sid,
      state: sid >= 0 ? 'pending' : 'local',
    };

    this.verified++;
    if (row.diff > this.bestDiff) {
      this.bestDiff = row.diff;
    }

    if (row.state === 'pending') {
      this.pending++;
      this.waiting.set(`${row.pool}:${sid}`, row);
    }

    if (!this.paused) {
      this.rows = [row, ...this.rows];
    }
  }

  private onVerdict(ev: any): void {
    const pool: number = ev.p ?? 0;
    const sid: number = ev.sid ?? -1;
    const ok: boolean = ev.ok === true;
    const reason: string | undefined = ev.why;

    if (sid >= 0) {
      this.settle(this.waiting.get(`${pool}:${sid}`), ok, reason);
      this.waiting.delete(`${pool}:${sid}`);
      return;
    }

    // SV2 acknowledges in batches without saying which sequence numbers it
    // covers, so resolve the oldest shares still waiting on this pool
    const count: number = ev.cnt ?? 1;
    const oldest = Array.from(this.waiting.entries())
      .filter(([, row]) => row.pool === pool)
      .sort((a, b) => a[1].ts - b[1].ts)
      .slice(0, count);

    for (const [key, row] of oldest) {
      this.settle(row, ok, reason);
      this.waiting.delete(key);
    }
  }

  private settle(row: ShareRow | undefined, ok: boolean, reason?: string): void {
    // the counters follow the pool even when the row already scrolled off
    if (ok) {
      this.accepted++;
    } else {
      this.rejected++;
    }

    if (!row || row.state !== 'pending') {
      return;
    }

    row.state = ok ? 'accepted' : 'rejected';
    row.reason = reason;
    this.pending--;
  }

  private onReset(ev: any): void {
    // the pool reconnected and its id counter restarted, so nothing still
    // waiting on it can ever be matched again
    const pool: number = ev.p ?? 0;

    for (const [key, row] of Array.from(this.waiting.entries())) {
      if (row.pool !== pool) {
        continue;
      }
      row.state = 'orphan';
      this.waiting.delete(key);
      this.pending--;
    }
  }
}

import { Injectable, OnDestroy } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';

export const WS_LOGS = '/api/ws';
export const WS_SHARES = '/api/v2/ws/shares';

@Injectable({
  providedIn: 'root'
})
export class WebsocketService implements OnDestroy {

  // one socket per endpoint: the log stream and the share stream are separate
  // handlers on the device and must not share a connection
  private sockets = new Map<string, WebSocketSubject<string>>();

  public connect(path: string = WS_LOGS): WebSocketSubject<string> {
    const existing = this.sockets.get(path);

    // Create socket if not present or already closed
    if (!existing || existing.closed) {
      const socket$ = webSocket<string>({
        url: `ws://${window.location.host}${path}`,
        deserializer: (e: MessageEvent) => e.data
      });
      this.sockets.set(path, socket$);
      return socket$;
    }
    return existing;
  }

  public close(path: string = WS_LOGS): void {
    const socket$ = this.sockets.get(path);
    if (!socket$) return;

    try {
      // 1) Try to close gracefully (sends a Close frame)
      if (!socket$.closed) {
        socket$.complete();
      }
      // 2) Ensure the underlying native WebSocket is closed as well.
      // RxJS WebSocketSubject wraps the native socket; in some shutdown scenarios
      // (tab close / reload) we want to be extra sure the connection is terminated.
      const ws: WebSocket | undefined = (socket$ as any)?._socket;
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        ws.close(1000, 'client closing');
      }
    } catch {
      // Ignore close errors — we just want to prevent leaking sockets
    } finally {
      this.sockets.delete(path);
    }
  }

  public closeAll(): void {
    for (const path of Array.from(this.sockets.keys())) {
      this.close(path);
    }
  }

  ngOnDestroy(): void {
    this.closeAll();
  }
}

import { invoke } from '@tauri-apps/api/core';
import { DesktopTransport } from './client';

import type { DesktopBootstrap } from './types';

interface TauriWindow extends Window {
  __TAURI_INTERNALS__?: object;
}

declare global {
  interface Window {
    __NORTHBRIDGE_DESKTOP__?: {
      bootstrap: DesktopBootstrap;
      transport: DesktopTransport;
    };
  }
}

export function isDesktopRuntime(): boolean {
  return typeof window !== 'undefined' && (window as TauriWindow).__TAURI_INTERNALS__ != null;
}

export async function initializeDesktopTransport(): Promise<DesktopTransport | null> {
  if (!isDesktopRuntime()) {
    return null;
  }
  if (window.__NORTHBRIDGE_DESKTOP__ != null) {
    return window.__NORTHBRIDGE_DESKTOP__.transport;
  }
  const bootstrap = await invoke<DesktopBootstrap>('desktop_bootstrap');
  const transport = new DesktopTransport(bootstrap);
  await transport.health();
  window.__NORTHBRIDGE_DESKTOP__ = { bootstrap, transport };
  return transport;
}

export function getDesktopTransport(): DesktopTransport | null {
  return typeof window === 'undefined' ? null : (window.__NORTHBRIDGE_DESKTOP__?.transport ?? null);
}

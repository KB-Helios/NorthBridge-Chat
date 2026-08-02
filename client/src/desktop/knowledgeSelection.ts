import { useSyncExternalStore } from 'react';

const STORAGE_KEY = 'northbridge.activeKnowledgeStackIds';
const EMPTY_SELECTION: string[] = [];
const listeners = new Set<() => void>();

let selection: string[] | null = null;

function readSelection(): string[] {
  if (selection != null) {
    return selection;
  }
  if (typeof window === 'undefined') {
    return EMPTY_SELECTION;
  }
  try {
    const value = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? '[]');
    selection = Array.isArray(value)
      ? [...new Set(value.filter((id): id is string => typeof id === 'string' && id !== ''))]
      : [];
  } catch {
    selection = [];
  }
  return selection;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getActiveKnowledgeStackIds(): string[] {
  return readSelection();
}

export function setActiveKnowledgeStackIds(ids: string[]): void {
  const next = [...new Set(ids.filter((id) => id !== ''))];
  selection = next;
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }
  listeners.forEach((listener) => listener());
}

export function useActiveKnowledgeStackIds(): string[] {
  return useSyncExternalStore(subscribe, readSelection, () => EMPTY_SELECTION);
}

import type { DesktopTransport } from '@northbridge/desktop-transport';

type ActiveDesktopGeneration = {
  conversationId: string;
  generationId: string;
  transport: DesktopTransport;
};

const activeGenerations = new Map<number, ActiveDesktopGeneration>();

export function registerDesktopGeneration(
  runIndex: number,
  generation: ActiveDesktopGeneration,
): void {
  activeGenerations.set(runIndex, generation);
}

export function clearDesktopGeneration(runIndex: number, generationId: string): void {
  if (activeGenerations.get(runIndex)?.generationId === generationId) {
    activeGenerations.delete(runIndex);
  }
}

export async function cancelDesktopGeneration(runIndex: number): Promise<boolean> {
  const generation = activeGenerations.get(runIndex);
  if (generation == null) {
    return false;
  }
  await generation.transport.cancelGeneration(generation.generationId);
  return true;
}

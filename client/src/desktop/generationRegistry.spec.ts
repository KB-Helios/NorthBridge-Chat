import type { DesktopTransport } from '@northbridge/desktop-transport';
import {
  cancelDesktopGeneration,
  clearDesktopGeneration,
  registerDesktopGeneration,
} from './generationRegistry';

describe('desktop generation registry', () => {
  it('cancels only the active run for a chat index', async () => {
    const transport = {
      cancelGeneration: jest.fn().mockResolvedValue({ status: 'cancelled' }),
    } as unknown as DesktopTransport;

    registerDesktopGeneration(3, {
      conversationId: 'conversation-1',
      generationId: 'generation-1',
      transport,
    });

    await expect(cancelDesktopGeneration(3)).resolves.toBe(true);
    expect(transport.cancelGeneration).toHaveBeenCalledWith('generation-1');

    clearDesktopGeneration(3, 'different-generation');
    await expect(cancelDesktopGeneration(3)).resolves.toBe(true);

    clearDesktopGeneration(3, 'generation-1');
    await expect(cancelDesktopGeneration(3)).resolves.toBe(false);
  });
});

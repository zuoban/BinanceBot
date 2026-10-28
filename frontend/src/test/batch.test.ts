import { expect, it, vi } from 'vitest';
import { runBatchControls } from '../components/Controls';
import { pair } from './fixtures';

it('submits all pause requests before waiting for any confirmation', async () => {
  let finishFirst!: (value: boolean) => void;
  const first = new Promise<boolean>(resolve => { finishFirst = resolve; });
  const run = vi.fn().mockReturnValueOnce(first).mockResolvedValueOnce(true);
  const pending = runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'pause', run);

  expect(run).toHaveBeenCalledTimes(2);
  expect(run).toHaveBeenNthCalledWith(2, expect.objectContaining({ symbol: 'BTCUSDC' }), 'pause', true);
  finishFirst(true);
  await pending;
});

it('continues submitting pauses if one control unexpectedly rejects', async () => {
  const run = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce(true);
  await expect(runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'pause', run)).rejects.toThrow('offline');
  expect(run).toHaveBeenCalledTimes(2);
});

import { expect, it, vi } from 'vitest';
import { runBatchControls } from '../components/Controls';
import { pair } from './fixtures';

it('submits all pause requests before waiting for any confirmation', async () => {
  let finishFirst!: (value: 'cancellation_unconfirmed') => void;
  const first = new Promise<'cancellation_unconfirmed'>(resolve => { finishFirst = resolve; });
  const run = vi.fn().mockReturnValueOnce(first).mockResolvedValueOnce('confirmed');
  const pending = runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'pause', run);

  expect(run).toHaveBeenCalledTimes(2);
  expect(run).toHaveBeenNthCalledWith(2, expect.objectContaining({ symbol: 'BTCUSDC' }), 'pause', true);
  finishFirst('cancellation_unconfirmed');
  await pending;
});

it('continues submitting pauses if one control unexpectedly rejects', async () => {
  const run = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce('confirmed');
  await expect(runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'pause', run)).resolves.toEqual([{ pair: pair('SOLUSDC'), result: 'failed' }]);
  expect(run).toHaveBeenCalledTimes(2);
});

it('distinguishes failed submissions, unknown pause states, unverified cancellations and locked targets', async () => {
  const run = vi.fn().mockResolvedValueOnce('failed').mockResolvedValueOnce('state_unconfirmed')
    .mockResolvedValueOnce('cancellation_unconfirmed').mockResolvedValueOnce('not_submitted');
  const targets = [pair('SOLUSDC'), pair('BTCUSDC'), pair('ETHUSDC'), pair('XRPUSDC')];
  await expect(runBatchControls(targets, 'pause', run)).resolves.toEqual([
    { pair: targets[0], result: 'failed' }, { pair: targets[1], result: 'state_unconfirmed' },
    { pair: targets[2], result: 'cancellation_unconfirmed' }, { pair: targets[3], result: 'not_submitted' },
  ]);
  expect(run).toHaveBeenCalledTimes(4);
});

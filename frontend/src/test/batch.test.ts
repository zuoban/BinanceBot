/// <reference lib="es2024.promise" />
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
  await expect(runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'pause', run)).resolves.toEqual([{ pair: pair('SOLUSDC'), result: 'request_unconfirmed' }]);
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

it('caps concurrent pause fan-out to 6 in-flight pairs while preserving input result order', async () => {
  const targets = Array.from({ length: 10 }, (_, i) => pair(`SYM${i}USDC`));
  const gates = targets.map(() => Promise.withResolvers<'cancellation_unconfirmed' | 'confirmed'>());
  let inFlight = 0;
  let peakInFlight = 0;
  const run = vi.fn().mockImplementation(async (target: { symbol: string }) => {
    const idx = targets.findIndex(item => item.symbol === target.symbol);
    inFlight++;
    peakInFlight = Math.max(peakInFlight, inFlight);
    try {
      return await gates[idx].promise;
    } finally {
      inFlight--;
    }
  });
  const pending = runBatchControls(targets, 'pause', run);
  expect(run).toHaveBeenCalledTimes(6);
  expect(peakInFlight).toBe(6);
  // Resolve later items before earlier ones to verify output order matches input order.
  for (let i = 5; i >= 0; i--) {
    gates[i].resolve(i % 2 === 0 ? 'cancellation_unconfirmed' : 'confirmed');
    await Promise.resolve();
  }
  for (let i = 9; i >= 6; i--) {
    gates[i].resolve(i % 2 === 0 ? 'cancellation_unconfirmed' : 'confirmed');
  }
  const outcomes = await pending;
  expect(peakInFlight).toBeLessThanOrEqual(6);
  expect(outcomes.map(item => item.pair.symbol)).toEqual(['SYM0USDC', 'SYM2USDC', 'SYM4USDC', 'SYM6USDC', 'SYM8USDC']);
});

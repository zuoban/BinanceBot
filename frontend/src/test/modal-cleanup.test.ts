import { expect, it, vi } from 'vitest';
import { destroyOpenModals, registerModalDestroyer } from '../lib/modalCleanup';

it('tracks multiple modal destroyers independently without clobbering', () => {
  const first = vi.fn();
  const second = vi.fn();
  const unregisterFirst = registerModalDestroyer(first);
  const unregisterSecond = registerModalDestroyer(second);

  destroyOpenModals();
  expect(first).toHaveBeenCalledOnce();
  expect(second).toHaveBeenCalledOnce();

  unregisterFirst();
  destroyOpenModals();
  expect(first).toHaveBeenCalledOnce();
  expect(second).toHaveBeenCalledTimes(2);

  unregisterSecond();
  destroyOpenModals();
  expect(second).toHaveBeenCalledTimes(2);
});

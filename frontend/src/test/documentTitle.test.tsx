import { renderHook } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';

it('updates the account title without resetting it between updates', () => {
  const setter = vi.spyOn(document, 'title', 'set');
  const { rerender, unmount } = renderHook(({ title }) => useDocumentTitle(title), { initialProps: { title: '100 | 90 USDC' } });
  setter.mockClear();

  rerender({ title: '101 | 90 USDC' });
  expect(setter).toHaveBeenCalledTimes(1);
  expect(setter).toHaveBeenCalledWith('101 | 90 USDC');
  expect(document.title).toBe('101 | 90 USDC');

  unmount();
  expect(document.title).toBe(defaultTitle);
  setter.mockRestore();
});

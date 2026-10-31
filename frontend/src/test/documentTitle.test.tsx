import { renderHook } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';

it('updates the account title without resetting it between updates', () => {
  const setter = vi.spyOn(document, 'title', 'set');
  const { rerender, unmount } = renderHook(({ title }: { title: string | null }) => useDocumentTitle(title), { initialProps: { title: '100 | 90%' as string | null } });
  setter.mockClear();

  rerender({ title: '101 | 90%' });
  expect(setter).toHaveBeenCalledTimes(1);
  expect(setter).toHaveBeenCalledWith('101 | 90%');
  expect(document.title).toBe('101 | 90%');
  rerender({ title: null });
  expect(document.title).toBe('101 | 90%');

  unmount();
  expect(document.title).toBe(defaultTitle);
  setter.mockRestore();
});

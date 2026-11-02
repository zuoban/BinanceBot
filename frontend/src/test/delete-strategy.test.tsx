import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { StrategyMenu } from '../components/StrategyMenu';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => ({ confirm: vi.fn(), success: vi.fn(), error: vi.fn(), api: vi.fn() }));
vi.mock('antd', () => ({
  App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) },
  Button: ({ children, 'aria-label': label }: React.PropsWithChildren<{ 'aria-label': string }>) => <button aria-label={label}>{children}</button>,
  Dropdown: ({ menu }: { menu: { items: { key?: string; label?: string }[]; onClick: (info: { key: string }) => void } }) => <div>{menu.items.filter(item => item.key).map(item => <button key={item.key} onClick={() => menu.onClick({ key: item.key! })}>{item.label}</button>)}</div>,
}));
vi.mock('../lib/api', () => ({ api: mocks.api }));
vi.mock('react-router-dom', () => ({ useNavigate: () => vi.fn() }));
vi.mock('../components/Controls', () => ({ useControls: () => ({ busy: new Set(), run: vi.fn() }) }));

beforeEach(() => vi.clearAllMocks());
function setup() {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  render(<QueryClientProvider client={client}><StrategyMenu pair={pair('BTCUSDC')} stale={false} /></QueryClientProvider>);
  return invalidate;
}

it('deletes only after confirmation and refreshes strategy data', async () => {
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockResolvedValue(null);
  const invalidate = setup();
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.api).toHaveBeenCalledWith('/api/pairs/BTCUSDC', { method: 'DELETE', body: JSON.stringify({ confirm: true }), signal: expect.any(AbortSignal) }));
  expect(mocks.confirm).toHaveBeenCalledOnce();
  expect(mocks.confirm.mock.invocationCallOrder[0]).toBeLessThan(mocks.api.mock.invocationCallOrder[0]);
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});

it('does not delete when cancelled and displays server errors', async () => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  mocks.api.mockRejectedValueOnce(new Error('请先暂停该策略'));
  const invalidate = setup();
  const user = userEvent.setup();
  const button = screen.getByRole('button', { name: '删除策略' });
  await user.click(button);
  expect(mocks.api).not.toHaveBeenCalled();
  await user.click(button);
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('请先暂停该策略'));
  expect(invalidate).not.toHaveBeenCalled();
});

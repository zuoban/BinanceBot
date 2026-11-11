/// <reference lib="es2024.promise" />
import { useState } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type * as Antd from 'antd';
import type * as Api from '../lib/api';
import { beforeEach, expect, it, vi } from 'vitest';
import { EditContext } from '../lib/session';
import type { ServerConfig } from '../lib/types';
import { MemoryRouter } from 'react-router-dom';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), confirm: vi.fn(), success: vi.fn(), error: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof Antd>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post };
});

const initial: ServerConfig = {
  version: 'v1',
  saved: { host: '127.0.0.1', port: 8080 },
  active: { host: '127.0.0.1', port: 8080 },
};
const globalConfig = { version: 'g1', recv_window: 5000, sync_interval_secs: 30, global_max_position_notional: null, pause_on_zero_available_margin: false, min_available_margin_pct: null };

beforeEach(() => {
  vi.clearAllMocks();
  mocks.api.mockReset();
  mocks.post.mockReset();
  mocks.confirm.mockReset().mockResolvedValue(true);
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper() {
    const [dirty, setDirty] = useState(false);
    const [saving, setSaving] = useState(false);
    return <QueryClientProvider client={client}><EditContext.Provider value={{ dirty, setDirty, saving, setSaving }}><MemoryRouter><Settings /></MemoryRouter></EditContext.Provider></QueryClientProvider>;
  }
  return { client, ...render(<Wrapper />) };
}

it('restores the saved server settings without a redundant post-save refetch', async () => {
  const saved: ServerConfig = { ...initial, version: 'v2', saved: { ...initial.saved, host: '127.0.0.2' } };
  const serverQuery = vi.fn().mockResolvedValueOnce(initial);
  mocks.api.mockImplementation((path: string) => path === '/api/server/config' ? serverQuery() : Promise.resolve(globalConfig));
  mocks.post.mockResolvedValueOnce(saved).mockResolvedValueOnce({ ...saved, version: 'v3', saved: { ...saved.saved, port: 8081 } });
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '服务设置' }));
  const host = await screen.findByLabelText('监听地址');
  await waitFor(() => expect(host).toHaveValue('127.0.0.1'));
  await user.clear(host);
  await user.type(host, '127.0.0.2');
  await user.click(screen.getByRole('button', { name: '保存监听设置' }));
  await waitFor(() => expect(mocks.success).toHaveBeenCalledWith('服务设置已保存，重启后生效'));
  expect(serverQuery).toHaveBeenCalledTimes(1);

  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  expect(host).toHaveValue('127.0.0.2');
  const port = screen.getByLabelText('端口');
  await user.clear(port);
  await user.type(port, '8081');
  await user.click(screen.getByRole('button', { name: '保存监听设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith('/api/server/config', { host: '127.0.0.2', port: 8081, baseline_version: 'v2' }));
}, 40000);

it('keeps the original values and version together when a background refresh returns newer settings', async () => {
  const newer: ServerConfig = { ...initial, version: 'v2', saved: { host: '127.0.0.2', port: 8082 } };
  const serverQuery = vi.fn().mockResolvedValueOnce(initial).mockResolvedValue(newer);
  mocks.api.mockImplementation((path: string) => path === '/api/server/config' ? serverQuery() : Promise.resolve(globalConfig));
  mocks.post.mockRejectedValue(new Error('服务设置已被其他请求修改，请刷新后重试'));
  const { client } = setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '服务设置' }));
  const host = await screen.findByLabelText('监听地址');
  await waitFor(() => expect(host).toHaveValue('127.0.0.1'));
  await user.clear(host);
  await user.type(host, '127.0.0.3');
  await act(async () => { await client.refetchQueries({ queryKey: ['server-config'] }); });

  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  expect(host).toHaveValue('127.0.0.1');
  const port = screen.getByLabelText('端口');
  expect(port).toHaveValue('8080');
  await user.clear(port);
  await user.type(port, '8081');
  await user.click(screen.getByRole('button', { name: '保存监听设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/server/config', { host: '127.0.0.1', port: 8081, baseline_version: 'v1' }));
  expect(await screen.findByText('服务设置已被其他请求修改，请刷新后重试')).toBeInTheDocument();
}, 40000);

it('clears validation errors when reverting dirty server settings', async () => {
  mocks.api.mockImplementation((path: string) => path === '/api/server/config' ? Promise.resolve(initial) : Promise.resolve(globalConfig));
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '服务设置' }));
  const host = await screen.findByLabelText('监听地址');
  await waitFor(() => expect(host).toHaveValue('127.0.0.1'));
  await user.clear(host);
  await user.click(screen.getByRole('button', { name: '保存监听设置' }));
  await waitFor(() => expect(host).toHaveAttribute('aria-invalid', 'true'));
  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  await waitFor(() => expect(host).toHaveValue('127.0.0.1'));
  await waitFor(() => expect(host).not.toHaveAttribute('aria-invalid', 'true'));
});

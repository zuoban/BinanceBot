import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import OverviewPage from '../pages/Overview';
import { snapshot } from './fixtures';
import type { Overview } from '../lib/types';

const overview = vi.hoisted(() => ({ data: undefined as Overview | undefined, error: null, dataUpdatedAt: Date.now(), isFetching: false, isPending: false, refetch: vi.fn() }));
beforeEach(() => { vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() }))); });
vi.mock('../lib/queries', () => ({ useOverview: () => overview }));
vi.mock('../components/Controls', () => ({ useControls: () => ({ busy: new Set(), results: {}, run: vi.fn() }), PairControls: () => null, runBatchControls: vi.fn() }));

it('summarizes statuses and lets the user clear a search with no results', async () => {
  const running = snapshot('BTCUSDC'); running.status = 'RUNNING';
  const error = snapshot('SOLUSDC'); error.status = 'ERROR';
  overview.data = { generated_at: new Date().toISOString(), pairs: [running, error].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })) };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const summary = screen.getByLabelText('策略运行概览');
  expect(within(summary).getByText('异常').parentElement).toHaveTextContent('1');
  const user = userEvent.setup();
  await user.type(screen.getByLabelText('搜索交易对'), 'NOTFOUND');
  expect(screen.getByText('没有符合筛选条件的策略')).toBeInTheDocument();
  await user.click(screen.getAllByRole('button', { name: '清除筛选' })[0]);
  expect(screen.getByText('BTCUSDC')).toBeInTheDocument();
});

it('offers creation when there are no strategies', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('暂无策略数据')).toBeInTheDocument();
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(2);
});

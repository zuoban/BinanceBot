import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import Login from '../pages/Login';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), setToken: vi.fn() }));
vi.mock('../lib/api', () => ({ api: mocks.api, post: mocks.post, tokenStore: { set: mocks.setToken } }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null }));

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});
function setup(initialized: boolean) {
  mocks.api.mockResolvedValue({ initialized });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><Login onLogin={vi.fn()} /></QueryClientProvider>);
}

it('requires a password before login', async () => {
  setup(true);
  const user = userEvent.setup();
  await user.click(await screen.findByRole('button', { name: '进入控制台' }));
  expect(await screen.findByText('请输入密码')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
});

it('validates confirmation during initial setup', async () => {
  setup(false);
  const user = userEvent.setup();
  await user.type(await screen.findByLabelText('初始化码'), 'example-code');
  await user.type(screen.getByLabelText('管理员密码'), 'password1');
  await user.type(screen.getByLabelText('确认密码'), 'password2');
  await user.click(screen.getByRole('button', { name: '创建并进入控制台' }));
  await waitFor(() => expect(screen.getByText('两次密码不一致')).toBeInTheDocument());
  expect(mocks.post).not.toHaveBeenCalled();
});

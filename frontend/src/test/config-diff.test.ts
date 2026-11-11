import { expect, it } from 'vitest';
import { describeNewGridConfig, diffGridConfig, sameGridConfig } from '../lib/configDiff';
import { pair } from './fixtures';

const base = () => { const { post_only: _postOnly, ...config } = pair('BTCUSDC').grid_config; return config; };

it('reports nothing when numeric strings only differ in formatting', () => {
  expect(diffGridConfig({ ...base(), grid_interval: '0.10', max_position_usdc: '1000' }, { ...base(), grid_interval: '0.1', max_position_usdc: '1000.0' })).toEqual([]);
});

it('treats an unset limit and an empty one as the same, but unset to a value as a change', () => {
  expect(diffGridConfig({ ...base(), min_price: null }, { ...base(), min_price: '' as unknown as null })).toEqual([]);
  expect(diffGridConfig({ ...base(), min_price: null }, { ...base(), min_price: '90' })).toEqual([
    expect.objectContaining({ key: 'min_price', label: '最低价格', before: '未设置', after: '90', risky: true }),
  ]);
});

it('flags direction and position-limit changes as risky and window changes as routine', () => {
  const changes = diffGridConfig({ ...base(), pause_on_max_position: true }, { ...base(), direction: 'SHORT', pause_on_max_position: false, buy_window: 9 });
  expect(changes.map(c => [c.key, c.risky])).toEqual([['direction', true], ['buy_window', false], ['pause_on_max_position', true]]);
  expect(changes[0]).toMatchObject({ before: '做多', after: '做空' });
});

it('deduplicates max-position policy diffs into a single row across all three policy modes', () => {
  const pausePolicy = { ...base(), max_position_usdc: '1000', pause_on_max_position: true, cancel_opening_on_max_position: false };
  const cancelOpeningPolicy = { ...base(), max_position_usdc: '1000', pause_on_max_position: false, cancel_opening_on_max_position: true };
  const limitOnlyPolicy = { ...base(), max_position_usdc: '1000', pause_on_max_position: false, cancel_opening_on_max_position: false };

  expect(diffGridConfig(pausePolicy, cancelOpeningPolicy)).toEqual([
    { key: 'pause_on_max_position', label: '持仓上限处理方式', before: '暂停策略', after: '仅撤开仓单', risky: true },
  ]);
  expect(diffGridConfig(limitOnlyPolicy, cancelOpeningPolicy)).toEqual([
    { key: 'pause_on_max_position', label: '持仓上限处理方式', before: '限制开仓', after: '仅撤开仓单', risky: true },
  ]);
  expect(sameGridConfig({ ...pausePolicy, post_only: true }, { ...pausePolicy, cancel_opening_on_max_position: true, post_only: true })).toBe(false);
  expect(sameGridConfig({ ...limitOnlyPolicy, post_only: true }, { ...cancelOpeningPolicy, post_only: true })).toBe(false);
  expect(describeNewGridConfig(cancelOpeningPolicy).filter(item => item.label.includes('上限'))).toEqual([
    { label: '持仓上限处理方式', risky: true, value: '仅撤开仓单' },
  ]);
});

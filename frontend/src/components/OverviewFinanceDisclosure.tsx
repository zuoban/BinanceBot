import { Card, Col, Row, Statistic } from 'antd';
import { ArrowRightOutlined } from '@ant-design/icons';
import { cnyText, compactNumber as number, ratioText } from '../lib/format';
import type { AccountGroup, PositionGroup } from '../lib/portfolio';
import { Pnl } from './Common';

export function OverviewFinanceDisclosure({
  accountList,
  positionList,
  stale,
  compactFinance,
  financesExpanded,
  setFinancesExpanded,
}: {
  accountList: AccountGroup[];
  positionList: PositionGroup[];
  stale: boolean;
  compactFinance: boolean;
  financesExpanded: boolean;
  setFinancesExpanded: (open: boolean) => void;
}) {
  if (!accountList.length && !positionList.length) return null;
  return <details className="finance-disclosure" open={!compactFinance || financesExpanded} onToggle={event => { if (compactFinance) setFinancesExpanded(event.currentTarget.open); }}>
    <summary className="finance-disclosure-summary">
      <span className="finance-disclosure-label">账户与持仓明细<ArrowRightOutlined aria-hidden="true" /></span>
      <span className={`finance-summary-values${stale ? ' is-stale' : ''}`}>
        {accountList.map(group => <span key={`account-${group.key}`}><small>账户权益 / {group.asset}</small><strong>{number(group.equity)}</strong></span>)}
        {positionList.map(group => <span key={`position-${group.key}`}><small>总持仓 / {group.asset}</small><strong>{number(group.total)}</strong></span>)}
      </span>
    </summary>
    <div className={`overview-finances${stale ? ' is-stale' : ''}`}>
      {accountList.length > 0 && <section className="overview-finance-group" aria-labelledby="accounts-heading">
        <div className="section-heading"><h2 id="accounts-heading">账户概览</h2></div>
        <Row gutter={[16, 16]}>
          {accountList.map(group => <Col span={24} key={group.key}><Card className="account-card finance-card" size="small">
            <div className="section-line finance-card-heading"><span>保证金余额</span></div>
            <div className="finance-card-primary"><Statistic value={group.equity ?? '--'} formatter={() => number(group.equity)} suffix={group.asset} />
              <span className="finance-card-note muted">{cnyText(group.cny, true)}</span></div>
            <div className="finance-metrics">
              <div><span>可用余额</span><strong>{number(group.available)} <small>{group.asset}</small></strong></div>
              <div><span>可用比例（按钱包余额）</span><strong>{ratioText(group.ratio, true)}</strong></div>
            </div>
            <div className="finance-metrics" aria-label={`${group.asset}账户资产`}>
              <div><span>钱包余额</span><strong>{number(group.wallet)}</strong></div>
              <div><span>未实现盈亏</span><strong><Pnl value={group.unrealized} /></strong></div>
            </div>
            <div className="muted">{group.count} 个策略共用账户 · 去重展示</div>
          </Card></Col>)}
        </Row>
      </section>}
      {positionList.length > 0 && <section className="overview-finance-group" aria-labelledby="positions-heading">
        <div className="section-heading"><h2 id="positions-heading">持仓概览</h2><span className="muted">按标记价格计算 · 全部策略</span></div>
        <Row gutter={[16, 16]}>
          {positionList.map(group => <Col span={24} key={group.key}>
            <Card className="position-summary finance-card" size="small">
              <div className="section-line finance-card-heading"><span>总持仓金额 / {group.asset}</span></div>
              <div className="finance-card-primary">
                <Statistic value={group.total ?? '--'} formatter={() => number(group.total)} suffix={group.asset} />
                <span className="finance-card-note muted">按标记价格估值</span>
              </div>
              <div className="finance-metrics">
                <div><span>做多持仓金额</span><strong className="position-long">{number(group.long)}</strong></div>
                <div><span>做空持仓金额</span><strong className="position-short">{number(group.short)}</strong></div>
              </div>
              <div className="finance-metrics">
                <div><span>多空比（金额占比）</span><strong>{group.total !== null && group.total > 0 && group.long !== null && group.short !== null ? `${number(group.long / group.total * 100, 1)}% / ${number(group.short / group.total * 100, 1)}%` : '--'}</strong></div>
                <div><span>覆盖策略</span><strong>{group.count} <small>个</small></strong></div>
              </div>
              <div className="position-summary-footer muted">按实际持仓方向统计{group.total === null ? ' · 部分持仓数据不可用' : ''}</div>
            </Card>
          </Col>)}
        </Row>
      </section>}
    </div>
  </details>;
}

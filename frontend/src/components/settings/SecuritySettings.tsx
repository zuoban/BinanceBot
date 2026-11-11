import { Alert, Card } from 'antd';

export function SecuritySettings() {
  return <Card title="管理员密码" className="settings-card">
    <Alert type="warning" showIcon title="管理员密码通过服务端启动参数配置，不保存在数据库中。" />
    <dl className="security-guide">
      <div>
        <dt>启动参数与环境变量</dt>
        <dd>使用命令行参数 <code>--admin-password &lt;密码&gt;</code>，或设置环境变量 <code>ADMIN_PASSWORD</code>，然后重启后端。</dd>
      </div>
      <div>
        <dt>访问边界与安全建议</dt>
        <dd>未设置密码时不启用认证，所有能访问该端口的人都可以操作控制台。请仅在受信任网络或本机使用，并优先通过环境变量提供密码，避免密码出现在进程列表中。</dd>
      </div>
      <div>
        <dt>密码轮换与会话失效</dt>
        <dd>修改密码后需重启后端；重启会清除所有登录会话，需要使用新密码重新登录。</dd>
      </div>
    </dl>
  </Card>;
}

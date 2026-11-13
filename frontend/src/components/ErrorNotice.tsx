import { Alert, Button } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';

export function ErrorNotice({ error, retry }: { error: Error | null; retry?: () => void }) {
  if (!error) return null;
  return (
    <Alert
      type="error"
      showIcon
      title={error.message}
      action={retry && <Button size="small" onClick={retry} icon={<ReloadOutlined />}>重试</Button>}
    />
  );
}

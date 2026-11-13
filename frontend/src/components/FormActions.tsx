import { useContext } from 'react';
import { App, Button, type FormInstance } from 'antd';
import { errorMessage } from '../lib/api';
import { useAsyncLock } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { EditContext } from '../lib/session';

export function isConfigConflict(error: Error | null | undefined, ...messageFragments: string[]): boolean {
  return Boolean(
    error && (
      ('status' in error && error.status === 409)
      || messageFragments.some(fragment => error.message.includes(fragment))
    ),
  );
}

function clearFormErrors(form: FormInstance) {
  const fields = form.getFieldsError().filter(item => item.errors.length > 0 || item.warnings.length > 0);
  if (fields.length > 0) {
    form.setFields(fields.map(item => ({ name: item.name, errors: [], warnings: [] })));
  }
}

export function ConflictReloadButton<T>({
  form,
  label,
  activeRef,
  resetKey,
  refetch,
  onLoaded,
  onReset,
}: {
  form?: FormInstance;
  label: string;
  activeRef: { current: unknown };
  resetKey?: unknown;
  refetch: () => Promise<{ data?: T; error: unknown }>;
  onLoaded: (data: NonNullable<T>) => boolean | void;
  onReset: () => void;
}) {
  const { message } = App.useApp();
  const { confirm } = useConfirmDialog(resetKey);
  const edit = useContext(EditContext);
  const runReload = useAsyncLock();
  return (
    <Button
      className="conflict-reload-btn"
      disabled={edit.saving}
      onClick={() => {
        const current = activeRef.current;
        if (edit.saving || !current) return;
        void runReload(async () => {
          const confirmed = await confirm({
            title: `重新加载${label}？`,
            content: '重新加载会放弃当前未保存的修改，请先记录需要保留的内容。',
            okText: '重新加载',
            cancelText: '继续编辑',
          });
          if (!confirmed || activeRef.current !== current) return;
          const result = await refetch();
          if (activeRef.current !== current) return;
          if (result.error) {
            message.error(errorMessage(result.error));
            return;
          }
          if (!result.data || onLoaded(result.data) === false) return;
          if (form) clearFormErrors(form);
          edit.setDirty(false);
          onReset();
        });
      }}
    >
      重新加载{label}
    </Button>
  );
}

export function FormRevertButton({
  form,
  activeRef,
  resetKey,
  onRevert,
  onReset,
}: {
  form?: FormInstance;
  activeRef: { current: unknown };
  resetKey?: unknown;
  onRevert: () => void;
  onReset: () => void;
}) {
  const { confirm } = useConfirmDialog(resetKey);
  const edit = useContext(EditContext);
  const runRevert = useAsyncLock();
  return (
    <Button
      onClick={() => {
        const current = activeRef.current;
        if (!current) return;
        void runRevert(async () => {
          if (edit.dirty && !await confirm({ title: '放弃未保存的修改？', okText: '还原', cancelText: '继续编辑' })) return;
          if (activeRef.current !== current) return;
          onRevert();
          if (form) clearFormErrors(form);
          edit.setDirty(false);
          onReset();
        });
      }}
    >
      还原
    </Button>
  );
}

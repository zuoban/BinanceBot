import { useCallback, useEffect, useRef } from 'react';
import { App, type ModalFuncProps } from 'antd';

interface ConfirmDialogHandle {
  destroy?: () => void;
  update?: (configUpdate: ModalFuncProps | ((prevConfig: ModalFuncProps) => ModalFuncProps)) => void;
}

interface ActiveConfirmDialog {
  handle: ConfirmDialogHandle;
  cancel: () => void;
}

export function useConfirmDialog(resetKey?: unknown) {
  const { modal } = App.useApp();
  const modalRef = useRef(modal);
  modalRef.current = modal;
  const activeRef = useRef(new Set<ActiveConfirmDialog>());
  const prevKeyRef = useRef<{ initialized: boolean; key: unknown }>({ initialized: false, key: resetKey });

  const destroyAll = useCallback(() => {
    const entries = Array.from(activeRef.current);
    activeRef.current.clear();
    for (const entry of entries) {
      entry.cancel();
      entry.handle.destroy?.();
    }
  }, []);

  useEffect(() => {
    if (prevKeyRef.current.initialized && !Object.is(prevKeyRef.current.key, resetKey)) {
      destroyAll();
    }
    prevKeyRef.current = { initialized: true, key: resetKey };
  }, [resetKey, destroyAll]);

  useEffect(() => () => destroyAll(), [destroyAll]);

  const confirmWithHandle = useCallback(
    (buildProps: (getHandle: () => ConfirmDialogHandle | null) => ModalFuncProps): Promise<boolean> =>
      new Promise<boolean>(resolve => {
        let currentHandle: ConfirmDialogHandle | null = null;
        let settled = false;
        let entry: ActiveConfirmDialog | null = null;
        const finish = (value: boolean) => {
          if (settled) return;
          settled = true;
          if (entry) activeRef.current.delete(entry);
          resolve(value);
        };
        const baseProps = buildProps(() => currentHandle);
        const userAfterClose = baseProps.afterClose;
        const raw = modalRef.current.confirm({
          ...baseProps,
          afterClose: () => {
            userAfterClose?.();
            finish(false);
          },
        }) as (ConfirmDialogHandle & PromiseLike<boolean>) | undefined;
        if (!raw) {
          finish(false);
          return;
        }
        currentHandle = raw;
        entry = { handle: raw, cancel: () => finish(false) };
        activeRef.current.add(entry);
        if (typeof raw.then === 'function') {
          raw.then(value => finish(Boolean(value)), () => finish(false));
        } else {
          finish(Boolean(raw));
        }
      }),
    [],
  );

  const confirm = useCallback(
    (props: ModalFuncProps): Promise<boolean> => confirmWithHandle(() => props),
    [confirmWithHandle],
  );

  return { confirm, confirmWithHandle, destroyAll };
}

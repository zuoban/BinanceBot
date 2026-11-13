import { useEffect, type ReactNode } from 'react';
import { App as AntApp, Modal } from 'antd';
import { registerModalDestroyer } from '../lib/modalCleanup';
import { ThemeProvider } from '../lib/theme';

export default function UiShell({ children }: { children: ReactNode }) {
  useEffect(() => registerModalDestroyer(() => Modal.destroyAll()), []);
  return <ThemeProvider><AntApp>{children}</AntApp></ThemeProvider>;
}

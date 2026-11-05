import { useEffect, useMemo, useRef, useState } from 'react';
import { Input, Modal } from 'antd';
import type { InputRef } from 'antd';
import { SearchOutlined } from '@ant-design/icons';

export interface Command {
  key: string;
  label: string;
  group: string;
  hint?: string;
  icon?: React.ReactNode;
  keywords?: string;
  disabled?: boolean;
  run: () => void;
}

export function CommandPalette({ open, onClose, commands }: { open: boolean; onClose: () => void; commands: Command[] }) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const input = useRef<InputRef>(null);
  const list = useRef<HTMLUListElement>(null);
  const results = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return commands.filter(command => !command.disabled && (!needle || `${command.label} ${command.keywords ?? ''} ${command.group}`.toLowerCase().includes(needle)));
  }, [commands, query]);
  useEffect(() => { if (open) { setQuery(''); setActive(0); } }, [open]);
  useEffect(() => { setActive(0); }, [query]);
  useEffect(() => { list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView?.({ block: 'nearest' }); }, [active]);
  const choose = (command: Command | undefined) => {
    if (!command) return;
    onClose();
    command.run();
  };
  const onKeyDown = (event: React.KeyboardEvent) => {
    // Enter that commits an IME candidate must not run a command.
    if (event.nativeEvent.isComposing || event.keyCode === 229) return;
    if (event.key === 'ArrowDown') { event.preventDefault(); setActive(index => results.length ? (index + 1) % results.length : 0); }
    else if (event.key === 'ArrowUp') { event.preventDefault(); setActive(index => results.length ? (index - 1 + results.length) % results.length : 0); }
    else if (event.key === 'Enter') { event.preventDefault(); choose(results[active]); }
  };
  let lastGroup = '';
  return <Modal open={open} aria-label="命令面板" onCancel={onClose} footer={null} closable={false} destroyOnHidden width={560} className="palette" afterOpenChange={visible => { if (visible) input.current?.focus(); }}>
    <div className="palette-search">
      <SearchOutlined aria-hidden="true" />
      <Input ref={input} variant="borderless" aria-label="搜索命令" placeholder="搜索交易对、页面或操作" value={query} onChange={event => setQuery(event.target.value)} onKeyDown={onKeyDown} role="combobox" aria-expanded={results.length > 0} aria-controls="palette-list" aria-activedescendant={results[active] ? `palette-${results[active].key}` : undefined} autoComplete="off" />
      <kbd>Esc</kbd>
    </div>
    <ul className="palette-list" id="palette-list" role="listbox" aria-label="命令列表" ref={list}>
      {results.map((command, index) => {
        const heading = command.group !== lastGroup ? command.group : null;
        lastGroup = command.group;
        return <li key={command.key} role="presentation">
          {heading && <div className="palette-group">{heading}</div>}
          <div id={`palette-${command.key}`} role="option" aria-selected={index === active} className={`palette-item${index === active ? ' palette-item-active' : ''}`} onMouseMove={() => setActive(index)} onClick={() => choose(command)}>
            <span className="palette-icon" aria-hidden="true">{command.icon}</span>
            <span className="palette-label">{command.label}</span>
            {command.hint && <span className="palette-hint">{command.hint}</span>}
          </div>
        </li>;
      })}
      {!results.length && <li className="palette-empty" role="presentation">没有匹配的命令</li>}
    </ul>
  </Modal>;
}

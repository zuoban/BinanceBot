import { useEffect } from 'react';

export const defaultTitle = 'Grid Console · 网格交易控制台';

export function useDocumentTitle(title: string) {
  useEffect(() => {
    if (document.title !== title) document.title = title;
  }, [title]);
  useEffect(() => () => { document.title = defaultTitle; }, []);
}

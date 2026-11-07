import '@testing-library/jest-dom/vitest';
import { cleanup, configure } from '@testing-library/react';
import { afterEach, beforeEach, vi } from 'vitest';

configure({ asyncUtilTimeout: 5000 });
beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});
afterEach(() => { cleanup(); localStorage.clear(); sessionStorage.clear(); });

import { expect, it } from 'vitest';
import { csvCell, toCsv } from '../lib/exportCsv';

it('quotes cells containing separators, quotes and line breaks', () => {
  expect(csvCell('a,b')).toBe('"a,b"');
  expect(csvCell('say "hi"')).toBe('"say ""hi"""');
  expect(csvCell('line1\nline2')).toBe('"line1\nline2"');
  expect(csvCell(null)).toBe('');
  expect(csvCell(12.5)).toBe('12.5');
});

it('neutralises formula-looking text so spreadsheets do not execute it', () => {
  expect(csvCell('=HYPERLINK("http://x")')).toBe('"\'=HYPERLINK(""http://x"")"');
  expect(csvCell('+1+1')).toBe("'+1+1");
  expect(csvCell('@SUM(A1)')).toBe("'@SUM(A1)");
  expect(csvCell('-2+3')).toBe("'-2+3");
  expect(csvCell('  =1+1')).toBe("'  =1+1");
  expect(csvCell('\n@SUM(A1)')).toBe('"\'\n@SUM(A1)"');
  expect(csvCell('safe-text')).toBe('safe-text');
});

it('joins rows with CRLF and keeps the header first', () => {
  expect(toCsv(['时间', '价格'], [['t1', '1.5'], ['t2', '=1']])).toBe("时间,价格\r\nt1,1.5\r\nt2,'=1");
});

it('keeps negative and signed decimals numeric', () => {
  expect(csvCell('-0.11')).toBe('-0.11');
  expect(csvCell(-3)).toBe('-3');
  expect(csvCell('+2.5')).toBe('+2.5');
  expect(csvCell('-1e3')).toBe("'-1e3");
});

/** A cell that a spreadsheet would execute as a formula must be neutralised before export. */
const risky = /^[\t\r]|^\s*[=+\-@]/;
/** A plain signed decimal is data, not a formula, and must stay numeric (negative PnL). */
const plainNumber = /^[+-]?\d+(\.\d+)?$/;

export function csvCell(value: unknown): string {
  let text = value === null || value === undefined ? '' : String(value);
  if (risky.test(text) && !plainNumber.test(text)) text = `'${text}`;
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function toCsv(header: string[], rows: unknown[][]): string {
  return [header, ...rows].map(row => row.map(csvCell).join(',')).join('\r\n');
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url; link.download = filename;
  document.body.append(link); link.click(); link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** Download as UTF-8 with a BOM so Excel reads the Chinese headers correctly. */
export function downloadCsv(filename: string, csv: string) {
  downloadBlob(new Blob(['\uFEFF', csv], { type: 'text/csv;charset=utf-8' }), filename);
}

/** Plain-text copy that also works on non-secure origins, where the async clipboard API is missing. */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return true; }
  } catch { /* Fall through to the textarea path. */ }
  const area = document.createElement('textarea');
  area.value = text; area.setAttribute('readonly', ''); area.style.position = 'fixed'; area.style.opacity = '0';
  document.body.append(area); area.select();
  try { return document.execCommand('copy'); } finally { area.remove(); }
}

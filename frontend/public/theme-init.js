(() => {
  let preference;
  try { preference = localStorage.getItem('binance_bot_theme'); } catch { /* Web Storage may be unavailable. */ }
  let prefersDark = false;
  try { prefersDark = Boolean(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches); } catch { /* matchMedia may be unavailable. */ }
  const dark = preference === 'dark' || (preference !== 'light' && prefersDark);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  document.documentElement.style.colorScheme = dark ? 'dark' : 'light';
  const themeColor = document.querySelector('meta[name="theme-color"]');
  const nextColor = themeColor && (dark ? themeColor.dataset.dark : themeColor.dataset.light);
  if (themeColor && nextColor) themeColor.content = nextColor;
})();

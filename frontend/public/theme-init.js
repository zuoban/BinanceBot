(() => {
  let preference;
  try { preference = localStorage.getItem('binance_bot_theme'); } catch { /* Web Storage may be unavailable. */ }
  const dark = preference === 'dark' || (preference !== 'light' && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  document.documentElement.style.colorScheme = dark ? 'dark' : 'light';
  const themeColor = document.querySelector('meta[name="theme-color"]');
  if (dark && themeColor && themeColor.dataset.dark) themeColor.content = themeColor.dataset.dark;
})();

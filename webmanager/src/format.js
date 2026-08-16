export function formatBytes(value) {
  const bytes = Number(value || 0);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const digits = unit === 0 || size >= 100 ? 0 : size >= 10 ? 1 : 2;
  return `${size.toFixed(digits)} ${units[unit]}`;
}

export function formatCounter(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? formatBytes(value) : '--';
}

export function formatRate(value) {
  if (value == null) return '--';
  const rate = Number(value);
  return Number.isFinite(rate) && rate >= 0 ? `${formatBytes(rate)}/s` : '--';
}

export function formatPercent(value, total) {
  const numerator = Number(value);
  const denominator = Number(total);
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0 || numerator < 0) return '--';
  const percent = numerator / denominator * 100;
  const digits = percent >= 100 ? 0 : percent >= 10 ? 1 : 2;
  return `${percent.toFixed(digits)}%`;
}

export function formatMicros(value) {
  const micros = Number(value || 0);
  if (!micros) return '--';
  if (micros < 1000) return `${micros} us`;
  if (micros < 1_000_000) return `${(micros / 1000).toFixed(micros < 10_000 ? 2 : 1)} ms`;
  return `${(micros / 1_000_000).toFixed(2)} s`;
}

export function formatTimestamp(value, locale = 'zh-CN') {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.valueOf())) return '--';
  return date.toLocaleString(locale, { hour12: false });
}

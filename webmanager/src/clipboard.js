function copyWithTextArea(value) {
  const field = document.createElement('textarea');
  field.value = value;
  field.setAttribute('readonly', '');
  field.className = 'copy-fallback';
  document.body.append(field);
  field.select();
  const copied = document.execCommand('copy');
  field.remove();
  return copied;
}

export async function copyIdentifier(value, {
  clipboard = globalThis.navigator?.clipboard,
  fallback = copyWithTextArea,
} = {}) {
  if (!value) return false;
  try {
    if (clipboard?.writeText) {
      await clipboard.writeText(value);
      return true;
    }
  } catch (_error) {
    // Browser permissions and plain HTTP can reject the Clipboard API.
  }
  return Boolean(fallback(value));
}
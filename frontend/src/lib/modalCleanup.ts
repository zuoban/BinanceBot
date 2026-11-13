const activeDestroyers = new Set<() => void>();

export function registerModalDestroyer(destroyer: () => void) {
  activeDestroyers.add(destroyer);
  return () => {
    activeDestroyers.delete(destroyer);
  };
}

export function destroyOpenModals() {
  for (const destroyer of activeDestroyers) {
    destroyer();
  }
}

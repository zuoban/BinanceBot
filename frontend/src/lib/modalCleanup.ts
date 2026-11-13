let activeDestroyer: (() => void) | null = null;

export function registerModalDestroyer(destroyer: () => void) {
  activeDestroyer = destroyer;
  return () => {
    if (activeDestroyer === destroyer) activeDestroyer = null;
  };
}

export function destroyOpenModals() {
  activeDestroyer?.();
}

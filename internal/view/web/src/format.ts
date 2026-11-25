// Formatting shared by the map and the drawers: times relative to the model's clock, durations, sizes and the
// words a task state and a signer are shown as.

export const abs = (iso: string | undefined): string =>
  iso ? iso.replace('T', ' ').replace(/(\.\d+)?Z$/, ' UTC') : '';

export const rel = (now: number, iso: string | undefined): string => {
  if (!iso) {
    return 'never';
  }
  const s = Math.max(0, (now - Date.parse(iso)) / 1000);
  if (s < 10) {
    return 'just now';
  }
  if (s < 60) {
    return Math.round(s) + ' s ago';
  }
  if (s < 3600) {
    return Math.round(s / 60) + ' min ago';
  }
  return Math.floor(s / 3600) + ' h ago';
};

export const dur = (ms: number): string => {
  if (ms < 1000) {
    return Math.round(ms) + ' ms';
  }
  if (ms < 60000) {
    return (ms / 1000).toFixed(1) + ' s';
  }
  const m = Math.floor(ms / 60000);
  const s = Math.round((ms % 60000) / 1000);
  return m < 60 ? m + ' min ' + s + ' s' : Math.floor(m / 60) + ' h ' + (m % 60) + ' min';
};

export const bytes = (n: number): string => {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return (i ? n.toFixed(1) : String(n)) + ' ' + units[i];
};

export const grouped = (n: number): string => n.toLocaleString('en-GB');

export const clock = (ms: number): string => abs(new Date(ms).toISOString()).slice(11, 16);

// signerName is the receiver or host a principal names: wherry.rcv.<receiver> or wherry.tnt.<tenant>.<host>.
const signerName = (principal: string): string =>
  principal.replace(/^wherry\.rcv\./, '').replace(/^wherry\.tnt\.[^.]+\./, '');

export const signedBy = (principal: string | undefined): string =>
  principal ? 'signed by ' + signerName(principal) : 'unsigned';

type BadgeColor = 'primary' | 'success' | 'danger' | 'warning' | 'default' | 'hollow';
export type HealthColor = 'primary' | 'success' | 'danger' | 'warning' | 'subdued';

const states: Record<string, [string, BadgeColor]> = {
  TASK_STATE_WORKING: ['working', 'primary'],
  TASK_STATE_COMPLETED: ['completed', 'success'],
  TASK_STATE_FAILED: ['failed', 'danger'],
  TASK_STATE_CANCELED: ['canceled', 'default'],
  TASK_STATE_REJECTED: ['rejected', 'warning'],
};

// stateWord is a task state as the page says it: the fixed words above, or the state's own word for the rest.
export const stateWord = (state: string): string =>
  states[state]?.[0] ?? state.replace(/^TASK_STATE_/, '').toLowerCase().replace(/_/g, ' ');

export const stateColor = (state: string): BadgeColor => states[state]?.[1] ?? 'default';

export const healthOf = (color: BadgeColor): HealthColor =>
  color === 'primary' || color === 'success' || color === 'danger' || color === 'warning' ? color : 'subdued';

export const stateOrder = ['TASK_STATE_WORKING', 'TASK_STATE_COMPLETED', 'TASK_STATE_FAILED', 'TASK_STATE_CANCELED', 'TASK_STATE_REJECTED'];

export const or = (value: string | undefined, placeholder = 'unknown'): string => (value ? value : placeholder);

// EUI's own @emotion/css instance is not a public export; the page needs its cache to set the CSP nonce on its sheet.
declare module '@elastic/eui/es/services/emotion/css' {
  import type { EmotionCache } from '@emotion/cache';
  export const cache: EmotionCache;
}

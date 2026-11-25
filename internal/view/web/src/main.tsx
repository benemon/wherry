import createCache from '@emotion/cache';
import { cache as packageCss } from '@emotion/css';
import { cache as euiCss } from '@elastic/eui/es/services/emotion/css';
import { setNonce } from 'get-nonce';
import { EuiProvider } from '@elastic/eui';
import { StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './fonts.css';
import './styles.css';

const darkQuery = window.matchMedia('(prefers-color-scheme: dark)');

// Root follows the system colour scheme while the page is open.
function Root() {
  const [dark, setDark] = useState(darkQuery.matches);
  useEffect(() => {
    const onChange = () => setDark(darkQuery.matches);
    darkQuery.addEventListener('change', onChange);
    return () => darkQuery.removeEventListener('change', onChange);
  }, []);
  return (
    <EuiProvider colorMode={dark ? 'dark' : 'light'} cache={cache}>
      <App />
    </EuiProvider>
  );
}

// The page's policy allows a runtime <style> only with the nonce the server wrote into the meta tag. Four
// injectors put styles in at runtime and each takes the nonce here, before the first render: EuiProvider's cache,
// which its default, global and utility styles share; the package-default @emotion/css instance, which EuiOverlayMask
// and the flyout overlay use; EUI's own @emotion/css instance, which EuiThemeProvider uses for the colour-mode class;
// and react-remove-scroll-bar, which a focus trap uses to lock scrolling and reads its nonce from get-nonce.
const nonce = document.querySelector('meta[name="csp-nonce"]')?.getAttribute('content') || '';
const cache = createCache({ key: 'eui', nonce });
packageCss.sheet.nonce = nonce;
euiCss.sheet.nonce = nonce;
setNonce(nonce);

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);

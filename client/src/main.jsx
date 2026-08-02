import './polyfills/regeneratorRuntime';
import { createRoot } from 'react-dom/client';
import { initializeDesktopTransport, isDesktopRuntime } from '@northbridge/desktop-transport';
import { setRequestOverride } from 'librechat-data-provider';
import { initializeI18n } from './locales/i18n';
import App from './App';
import '@librechat/client/style.css';
import './style.css';
import './mobile.css';
import DesktopStartupError from './components/Desktop/StartupError';
import { createDesktopRequestOverride } from './desktop/requestOverride';
import { ApiErrorBoundaryProvider } from './hooks/ApiErrorBoundaryContext';
import 'katex/dist/katex.min.css';
import 'katex/dist/contrib/copy-tex.js';

window.addEventListener('vite:preloadError', (event) => {
  if (window.__lcRecoverStaleAssets?.()) {
    event.preventDefault();
  }
});

const container = document.getElementById('root');
const root = createRoot(container);

async function bootstrap() {
  await initializeI18n();
  try {
    const desktopTransport = await initializeDesktopTransport();
    if (desktopTransport != null) {
      setRequestOverride(createDesktopRequestOverride(desktopTransport));
    }
  } catch (error) {
    console.error('[desktop] Failed to initialize the local runtime', error);
    if (isDesktopRuntime()) {
      root.render(
        <ApiErrorBoundaryProvider>
          <DesktopStartupError />
        </ApiErrorBoundaryProvider>,
      );
      return;
    }
    throw error;
  }

  root.render(
    <ApiErrorBoundaryProvider>
      <App />
    </ApiErrorBoundaryProvider>,
  );
}

bootstrap().catch((error) => {
  console.error('[i18n] Failed to initialize before render', error);
  root.render(
    <ApiErrorBoundaryProvider>
      <App />
    </ApiErrorBoundaryProvider>,
  );
});

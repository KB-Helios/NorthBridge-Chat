import React from 'react';
import { useLocalize } from '~/hooks';

export default function StartupError() {
  const localize = useLocalize();

  return (
    <main className="flex min-h-screen items-center justify-center bg-surface-primary px-6 text-text-primary">
      <section
        className="w-full max-w-lg rounded-2xl border border-border-light bg-surface-secondary p-8 text-center shadow-xl"
        role="alert"
        aria-labelledby="desktop-startup-title"
      >
        <img className="mx-auto mb-6 h-16 w-16" src="/assets/logo.svg" alt="" />
        <h1 id="desktop-startup-title" className="mb-3 text-2xl font-semibold">
          {localize('com_ui_desktop_startup_title')}
        </h1>
        <p className="mb-6 text-sm leading-6 text-text-secondary">
          {localize('com_ui_desktop_startup_description')}
        </p>
        <button
          type="button"
          className="rounded-lg bg-surface-submit px-5 py-2.5 font-medium text-white hover:bg-surface-submit-hover focus:outline-none focus:ring-2 focus:ring-ring-primary focus:ring-offset-2"
          onClick={() => window.location.reload()}
        >
          {localize('com_ui_desktop_startup_retry')}
        </button>
      </section>
    </main>
  );
}

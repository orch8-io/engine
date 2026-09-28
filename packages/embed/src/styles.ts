/**
 * Base stylesheet shared by every element. Public tokens are `--orch8-*`
 * (light) and `--orch8-dark-*` (dark overrides); everything else is private.
 * Default colours meet WCAG AA (4.5:1) for text on their backgrounds.
 */
export const baseCss = `
:host {
  --_font: var(--orch8-font, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif);
  --_radius: var(--orch8-radius, 8px);
  --_space: var(--orch8-space, 8px);
  --_bg: var(--orch8-bg, #ffffff);
  --_surface: var(--orch8-surface, #f6f7f9);
  --_fg: var(--orch8-fg, #1a1d23);
  --_muted: var(--orch8-muted, #555c69);
  --_border: var(--orch8-border, #d5d9e0);
  --_accent: var(--orch8-accent, #3346c4);
  --_accent-fg: var(--orch8-accent-fg, #ffffff);
  --_success: var(--orch8-success, #1b6e36);
  --_danger: var(--orch8-danger, #b42318);
  --_warning: var(--orch8-warning, #7a4f00);
  --_info: var(--orch8-info, #1d5fa8);
  --_focus: var(--orch8-focus, #3346c4);
  display: block;
  box-sizing: border-box;
  color: var(--_fg);
  background: var(--_bg);
  font-family: var(--_font);
  font-size: var(--orch8-font-size, 14px);
  line-height: 1.5;
  border: 1px solid var(--_border);
  border-radius: var(--_radius);
  padding: calc(var(--_space) * 2);
  color-scheme: light;
}
:host([hidden]) { display: none; }
:host([color-scheme="dark"]) {
  --_bg: var(--orch8-dark-bg, #111418);
  --_surface: var(--orch8-dark-surface, #1b1f26);
  --_fg: var(--orch8-dark-fg, #e8eaee);
  --_muted: var(--orch8-dark-muted, #a6adb9);
  --_border: var(--orch8-dark-border, #333a45);
  --_accent: var(--orch8-dark-accent, #9db0ff);
  --_accent-fg: var(--orch8-dark-accent-fg, #0b1020);
  --_success: var(--orch8-dark-success, #6fd08f);
  --_danger: var(--orch8-dark-danger, #ff9b8f);
  --_warning: var(--orch8-dark-warning, #f2bd57);
  --_info: var(--orch8-dark-info, #8cc3ff);
  --_focus: var(--orch8-dark-focus, #9db0ff);
  color-scheme: dark;
}
@media (prefers-color-scheme: dark) {
  :host(:not([color-scheme="light"])) {
    --_bg: var(--orch8-dark-bg, #111418);
    --_surface: var(--orch8-dark-surface, #1b1f26);
    --_fg: var(--orch8-dark-fg, #e8eaee);
    --_muted: var(--orch8-dark-muted, #a6adb9);
    --_border: var(--orch8-dark-border, #333a45);
    --_accent: var(--orch8-dark-accent, #9db0ff);
    --_accent-fg: var(--orch8-dark-accent-fg, #0b1020);
    --_success: var(--orch8-dark-success, #6fd08f);
    --_danger: var(--orch8-dark-danger, #ff9b8f);
    --_warning: var(--orch8-dark-warning, #f2bd57);
    --_info: var(--orch8-dark-info, #8cc3ff);
    --_focus: var(--orch8-dark-focus, #9db0ff);
    color-scheme: dark;
  }
}
*, *::before, *::after { box-sizing: inherit; }
.sr-only {
  position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
  overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; border: 0;
}
:focus-visible { outline: 2px solid var(--_focus); outline-offset: 2px; }
.header { display: flex; align-items: center; justify-content: space-between; gap: var(--_space); margin-bottom: calc(var(--_space) * 1.5); flex-wrap: wrap; }
.title { font-size: 1.1em; font-weight: 600; margin: 0; display: flex; align-items: center; gap: var(--_space); }
.logo { max-height: 24px; max-width: 120px; }
.muted { color: var(--_muted); }
.small { font-size: 0.875em; }
button, .btn {
  font: inherit; cursor: pointer; border-radius: calc(var(--_radius) - 2px);
  border: 1px solid var(--_border); background: var(--_surface); color: var(--_fg);
  padding: calc(var(--_space) * 0.5) calc(var(--_space) * 1.5); min-height: 32px;
  text-decoration: none; display: inline-flex; align-items: center; gap: 4px;
}
button:hover:not(:disabled) { border-color: var(--_accent); }
button.primary { background: var(--_accent); color: var(--_accent-fg); border-color: var(--_accent); }
button:disabled { cursor: not-allowed; opacity: 0.6; }
button.link { background: none; border: none; color: var(--_accent); padding: 0; min-height: 0; text-decoration: underline; text-align: start; }
a { color: var(--_accent); }
input, select, textarea {
  font: inherit; color: var(--_fg); background: var(--_bg);
  border: 1px solid var(--_border); border-radius: calc(var(--_radius) - 2px);
  padding: calc(var(--_space) * 0.5) var(--_space); width: 100%;
}
textarea { font-family: var(--orch8-mono-font, ui-monospace, SFMono-Regular, Menlo, monospace); min-height: 72px; resize: vertical; }
[aria-invalid="true"] { border-color: var(--_danger); }
label { display: block; font-weight: 500; margin-bottom: 2px; }
.field { margin-bottom: var(--_space); }
.field-error { color: var(--_danger); font-size: 0.875em; margin-top: 2px; }
.state { display: inline-flex; align-items: center; gap: 4px; font-weight: 500; white-space: nowrap; }
.state::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: currentColor; flex: none; }
.state-completed { color: var(--_success); }
.state-failed, .state-cancelled { color: var(--_danger); }
.state-running, .state-scheduled { color: var(--_info); }
.state-waiting, .state-paused { color: var(--_warning); }
.state-pending, .state-skipped { color: var(--_muted); }
.status { padding: calc(var(--_space) * 2); text-align: center; color: var(--_muted); }
.status[role="alert"] { color: var(--_danger); }
.status .detail { display: block; font-size: 0.875em; color: var(--_muted); margin-top: 4px; }
.status button { margin-top: var(--_space); }
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid var(--_border); border-top-color: var(--_accent); border-radius: 50%; animation: spin 0.8s linear infinite; vertical-align: middle; margin-inline-end: 6px; }
@keyframes spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .spinner { animation: none; } }
.banner { font-size: 0.875em; color: var(--_warning); margin-bottom: var(--_space); }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: start; padding: var(--_space); border-bottom: 1px solid var(--_border); vertical-align: top; }
th { font-weight: 600; color: var(--_muted); font-size: 0.875em; }
ol.plain, ul.plain { list-style: none; margin: 0; padding: 0; }
.footer { display: flex; justify-content: space-between; align-items: center; margin-top: calc(var(--_space) * 1.5); gap: var(--_space); flex-wrap: wrap; }
.badge-link { font-size: 0.75em; color: var(--_muted); text-decoration: none; display: inline-flex; align-items: center; gap: 4px; }
.badge-link:hover { color: var(--_fg); text-decoration: underline; }
.badge-mark { width: 12px; height: 12px; border-radius: 3px; background: var(--_accent); display: inline-block; }
`;

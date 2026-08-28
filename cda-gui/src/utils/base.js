// base.js
export function getBasePath() {
  const configuredRoot = import.meta.env.VITE_CDA_API_ROOT || "/cwms-data";
  return new URL(configuredRoot, window.location.origin).pathname.replace(/\/$/, "");
}

export function getOrigin() {
  const { origin } = window.location;
  return origin;
}

export function getPathname() {
  const { pathname } = window.location;
  return pathname;
}

// base.js
export function getBasePath() {
  const { pathname } = window.location;
  const basePath = "/" + pathname.split("/")[1];
  return basePath;
}

export function getOrigin() {
  const { origin } = window.location;
  return origin;
}

export function getPathname() {
  const { pathname } = window.location;
  return pathname;
}

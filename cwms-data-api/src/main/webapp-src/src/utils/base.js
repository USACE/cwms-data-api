// base.js
export function getBasePath() {
    const { origin, pathname } = window.location;
    const basePath = "/" + pathname.split('/')[1];
    return basePath;
}

export function getOrigin() {
    const { origin, pathname } = window.location;
    return origin;
}

export function getPathname() {
    const { origin, pathname } = window.location;
    return pathname;
}


const apiRoot = import.meta.env.VITE_CDA_API_ROOT.replace(/\/$/, "");

export async function request(path, token, options = {}) {
  const response = await fetch(`${apiRoot}${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`.trim();
    try {
      const payload = await response.json();
      detail = payload.message ?? payload.detail ?? detail;
    } catch {
      // A proxy or gateway error may not contain CDA's JSON error envelope.
    }
    throw new Error(detail);
  }
  return response.status === 204 ? null : response.json();
}

export function userListsFrom(payload) {
  return payload?.["user-lists"] ?? payload?.entries ?? payload ?? [];
}

import { AuthorizationApi, Configuration } from "cwmsjs";

// Read raw responses: CDA dates include [UTC], which the generated model's
// Date constructor cannot parse, and DELETE returns an empty 204 response.
export function createApiKeyClient(basePath, token, fetchApi = fetch) {
  const api = new AuthorizationApi(
    new Configuration({
      basePath: basePath.replace(/\/$/, ""),
      fetchApi,
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    }),
  );
  const options = { cache: "no-store" };
  return {
    async list(signal) {
      const response = await api.getAuthKeysRaw({ ...options, signal });
      return response.raw.json();
    },
    async get(keyName, signal) {
      const response = await api.getAuthKeysWithKeyNameRaw(
        { keyName },
        { ...options, signal },
      );
      return response.raw.json();
    },
    async create(userId, keyName, expires, signal) {
      const response = await api.postAuthKeysRaw(
        {
          apiKey: {
            userId,
            keyName,
            expires: expires ? new Date(expires) : undefined,
          },
        },
        { ...options, signal },
      );
      return response.raw.json();
    },
    async revoke(keyName, signal) {
      await api.deleteAuthKeysWithKeyNameRaw({ keyName }, { ...options, signal });
    },
  };
}

export async function keyError(error) {
  const status = error?.response?.status;
  if (status === 401) return "Your session has expired. Sign out and sign in again.";
  if (status === 403)
    return "CDA denied access. Sign in with your user account and check your CWMS access with your office administrator. API keys cannot manage keys.";
  if (status === 404) return "This key no longer exists. Refresh your keys.";
  if (status === 409)
    return "A key with this name already exists. Choose another name.";
  // Do not render server response bodies, which may contain submitted credentials.
  return status
    ? `The request failed (HTTP ${status}). Refresh and try again.`
    : "Unable to reach CDA. Check your connection and try again.";
}

export function keyDate(value) {
  if (!value) return null;
  const date = new Date(value.replace(/\[[^\]]+\]$/, ""));
  return Number.isNaN(date.getTime()) ? null : date;
}

export function keyStatus(key, now = Date.now()) {
  if (!key.expires) return "No expiration";
  const expires = keyDate(key.expires);
  return !expires
    ? "Unknown expiration"
    : expires.getTime() <= now
      ? "Expired"
      : "Active";
}

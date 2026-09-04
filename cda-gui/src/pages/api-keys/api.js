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
  if (status === 401)
    return "Your sign-in could not be verified. Sign out and sign in again.";
  if (status === 403)
    return "CDA denied access. Sign in with your user account and check your CWMS access with your office administrator. API keys cannot manage keys.";
  if (status === 404) return "This key no longer exists. Refresh your keys.";
  if (status === 409)
    return "A key with this name already exists. Choose another name.";
  if (status >= 500)
    return "CDA could not complete the request. Refresh your keys to check whether the change was saved before trying again. If this continues, contact your office administrator.";
  if (status === 400 || status === 422)
    return "CDA could not accept these key details. Check the name and expiration date and try again.";
  if (status === 429)
    return "Too many requests were sent. Wait a moment, then try again.";
  // Do not render server response bodies, which may contain submitted credentials.
  return status
    ? "The request could not be completed. Refresh your keys and try again."
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

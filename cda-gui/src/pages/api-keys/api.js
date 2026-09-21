import { AuthorizationApi, Configuration } from "cwmsjs";

class KeyInputError extends Error {}

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
      if (!userId?.trim() || !keyName?.trim()) {
        throw new KeyInputError(
          "A signed-in user and a nonblank key name are required.",
        );
      }
      const expiration = expires ? keyDate(expires) : null;
      if (expires && !expiration) {
        throw new KeyInputError(
          "Choose a valid expiration date, or clear it for no expiration.",
        );
      }
      const response = await api.postAuthKeysRaw(
        {
          apiKey: {
            userId,
            keyName,
            expires: expiration ?? undefined,
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

export async function keyAccessDenied(error) {
  if (error?.response?.status !== 403) return null;
  const missingRoles = [];
  try {
    const { message } = await error.response.clone().json();
    if (typeof message === "string" && message.startsWith("Missing roles {")) {
      for (const role of ["CWMS Users", "cac_auth"]) {
        if (message.includes(`Role{name='${role}'}`)) missingRoles.push(role);
      }
    }
  } catch {
    // Proxies may omit CDA's role details. Still explain the route requirements.
  }
  return { missingRoles };
}

export async function keyError(error) {
  if (error instanceof KeyInputError) return error.message;
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
  if (status === 400 || status === 422) {
    // Only translate recognized CDA validation messages. Never echo arbitrary
    // response text, submitted values, SQL details, or stack traces into the UI.
    try {
      const body = await error.response.clone().json();
      const message = body?.message;
      if (
        message ===
        "expires must be a valid date/time string, for example 2030-01-01T00:00:00Z."
      )
        return "Choose a valid expiration date, or clear it for no expiration.";
      if (message === "user-id and key-name are required and must not be blank.")
        return "Enter a key name. If your profile is missing, sign in again.";
      if (message === "Request body must be a valid API key JSON object.")
        return "CDA could not read the key details. Refresh the page and try again.";
      if (
        typeof message === "string" &&
        /^One or more provided values exceeds the maximum length for the parameter\. The field KEY_NAME with provided length of \d+ has a maximum length of 64 characters\.$/.test(
          message,
        )
      )
        return "Key names must be 64 characters or fewer.";
    } catch {
      // Older servers and proxies may return non-JSON errors.
    }
    return "CDA could not accept these key details. Check the name and expiration date and try again.";
  }
  if (status === 429)
    return "Too many requests were sent. Wait a moment, then try again.";
  // Do not render server response bodies, which may contain submitted credentials.
  return status
    ? "The request could not be completed. Refresh your keys and try again."
    : "Unable to reach CDA. Check your connection and try again.";
}

export function keyDate(value) {
  if (!value) return null;
  if (typeof value !== "string") return null;
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

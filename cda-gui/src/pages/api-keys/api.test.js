import assert from "node:assert/strict";
import test from "node:test";
import { createApiKeyClient, keyDate, keyError, keyStatus } from "./api.js";

test("cwmsjs sends bearer auth, encodes names, and preserves CDA dates", async () => {
  const calls = [];
  const key = {
    "user-id": "TEST",
    "key-name": "report / #1",
    created: "2026-09-01T00:00:00+0000[UTC]",
  };
  const client = createApiKeyClient(
    "https://example.test/cwms-data/",
    "test-token",
    async (url, init) => {
      calls.push({ url, init });
      return new Response(JSON.stringify(url.endsWith("/keys") ? [key] : key), {
        status: 200,
      });
    },
  );
  assert.deepEqual(await client.list(), [key]);
  assert.deepEqual(await client.get(key["key-name"]), key);
  assert.equal(
    calls[1].url,
    "https://example.test/cwms-data/auth/keys/report%20%2F%20%231",
  );
  for (const { init } of calls) {
    assert.equal(init.headers.Authorization, "Bearer test-token");
    assert.equal(init.cache, "no-store");
  }
});

test("create serializes the current user and expiration without an office or supplied secret", async () => {
  let request;
  const client = createApiKeyClient(
    "https://example.test",
    "test-token",
    async (url, init) => {
      request = { url, init };
      return new Response(JSON.stringify({ "api-key": "test-only-secret" }), {
        status: 201,
      });
    },
  );
  assert.equal(
    (await client.create("TEST", "report", "2026-12-01T00:00:00Z"))["api-key"],
    "test-only-secret",
  );
  assert.equal(request.init.method, "POST");
  assert.deepEqual(JSON.parse(request.init.body), {
    "user-id": "TEST",
    "key-name": "report",
    expires: "2026-12-01T00:00:00.000Z",
  });
  await client.create("TEST", "report", null);
  assert.deepEqual(JSON.parse(request.init.body), {
    "user-id": "TEST",
    "key-name": "report",
  });
});

test("revoke accepts CDA's empty 204 and forwards cancellation", async () => {
  const controller = new AbortController();
  const client = createApiKeyClient(
    "https://example.test",
    "test-token",
    async (url, init) => {
      assert.equal(init.method, "DELETE");
      assert.equal(init.signal, controller.signal);
      return new Response(null, { status: 204 });
    },
  );
  await client.revoke("report", controller.signal);
});

test("CDA timezone dates distinguish expired, active, missing and malformed expiration", () => {
  assert.equal(
    keyDate("2026-09-01T00:00:00+0000[UTC]").toISOString(),
    "2026-09-01T00:00:00.000Z",
  );
  assert.equal(
    keyStatus({ expires: "2026-09-01T00:00:00+0000[UTC]" }, Date.parse("2026-09-02")),
    "Expired",
  );
  assert.equal(
    keyStatus({ expires: "2026-12-01T00:00:00Z" }, Date.parse("2026-09-02")),
    "Active",
  );
  assert.equal(keyStatus({}), "No expiration");
  assert.equal(keyStatus({ expires: "bad-date" }), "Unknown expiration");
});

test("error messages explain auth failures without displaying response bodies", async () => {
  for (const status of [400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504, 418]) {
    const message = await keyError({
      response: new Response("secret-must-not-be-shown", { status }),
    });
    assert.ok(!message.includes("secret-must-not-be-shown"));
    assert.ok(message.length > 20);
    assert.ok(!/\b[45]\d\d\b|HTTP|interactively/.test(message));
  }
});

test("all key operations catch generic server failures with a useful message", async () => {
  const client = createApiKeyClient(
    "https://example.test",
    "test-token",
    async () => new Response("Internal server error", { status: 500 }),
  );
  for (const request of [
    () => client.list(),
    () => client.get("test"),
    () => client.create("TEST", "test", null),
    () => client.revoke("test"),
  ]) {
    await assert.rejects(request, (asyncError) =>
      Boolean(asyncError.response?.status === 500),
    );
  }
  assert.match(
    await keyError({ response: { status: 500 } }),
    /Refresh your keys to check whether the change was saved/,
  );
});

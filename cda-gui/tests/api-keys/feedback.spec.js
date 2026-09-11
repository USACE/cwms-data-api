import { expect, test } from "@playwright/test";
import process from "node:process";

const original = {
  "user-id": "TEST",
  "key-name": "daily-report",
  created: "2026-01-01T00:00:00+0000[Z]",
  expires: null,
};

async function setup(page, failure = null) {
  const state = { keys: [{ ...original }], failure, requests: [] };
  await page.route("**/protocol/openid-connect/token", (route) =>
    route.fulfill({ json: { access_token: "test-token" } }),
  );
  await page.route("**/cwms-data/user/profile", (route) =>
    route.fulfill({ json: { "user-name": "TEST", roles: { SWT: ["CWMS Users"] } } }),
  );
  await page.route(/\/cwms-data\/auth\/keys(?:\/.*)?$/, async (route) => {
    const request = route.request();
    const method = request.method();
    const name = decodeURIComponent(
      new URL(request.url()).pathname.split("/")[4] ?? "",
    );
    state.requests.push(method);
    if (state.failure?.method === method) {
      return route.fulfill({
        status: state.failure.status,
        json: { message: state.failure.message ?? "System Error" },
      });
    }
    if (method === "POST") {
      const input = request.postDataJSON();
      if (state.keys.some((key) => key["key-name"] === input["key-name"]))
        return route.fulfill({ status: 409, json: { message: "Already exists" } });
      const key = { ...original, ...input };
      state.keys.unshift(key);
      return route.fulfill({
        status: 201,
        json: { ...key, "api-key": "synthetic-one-time-secret" },
      });
    }
    if (method === "DELETE") {
      state.keys = state.keys.filter((key) => key["key-name"] !== name);
      return route.fulfill({ status: 204 });
    }
    const key = state.keys.find((item) => item["key-name"] === name);
    return route.fulfill({
      status: name && !key ? 404 : 200,
      json: name ? (key ?? { message: "Not found" }) : state.keys,
    });
  });
  await page.goto("/cwms-data/");
  await page.getByRole("button", { name: /login|sign in/i }).click();
  await page.getByRole("link", { name: "API Keys", exact: true }).click();
  if (failure?.status === 403) return state;
  await expect(
    page.getByRole("button", { name: "Create key", exact: true }),
  ).toBeEnabled();
  return state;
}

const toast = (page) => page.locator(".api-key-toast:visible");

for (const roles of [["CWMS Users"], ["cac_auth"], ["CWMS Users", "cac_auth"]]) {
  test(`missing ${roles.join(" and ")} shows the permission page and recovers`, async ({
    page,
  }) => {
    const state = await setup(page, {
      method: "GET",
      status: 403,
      message: `Missing roles {${roles.map((role) => `Role{name='${role}'}`).join(",")}}`,
    });
    await expect(
      page.getByRole("heading", { name: "API key access required" }),
    ).toBeVisible();
    await expect(page.getByRole("alert")).toContainText("Reach out to your CWMS Admin");
    for (const role of roles)
      await expect(page.getByRole("listitem").filter({ hasText: role })).toContainText(
        "missing from your current",
      );
    await expect(
      page.getByRole("button", { name: "Create key", exact: true }),
    ).toHaveCount(0);
    expect(state.requests).toEqual(["GET"]);
    if (process.env.API_KEYS_SCREENSHOT_DIR && roles.length === 2) {
      await page.screenshot({
        path: `${process.env.API_KEYS_SCREENSHOT_DIR}/api-keys-permissions.png`,
        fullPage: true,
      });
      await page.setViewportSize({ width: 390, height: 844 });
      await expect(page.getByRole("alert")).toBeVisible();
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBeLessThanOrEqual(390);
      await page.screenshot({
        path: `${process.env.API_KEYS_SCREENSHOT_DIR}/api-keys-permissions-mobile.png`,
        fullPage: true,
      });
    }
    state.failure = { method: "GET", status: 503 };
    await page.getByRole("button", { name: "Check access again" }).click();
    await expect(toast(page)).toContainText("CDA could not complete");
    await expect(
      page.getByRole("heading", { name: "API key access required" }),
    ).toBeVisible();
    state.failure = null;
    await page.getByRole("button", { name: "Check access again" }).click();
    await expect(
      page.getByRole("button", { name: "Create key", exact: true }),
    ).toBeEnabled();
  });
}

test("permission loss during creation replaces the dialog with the warning page", async ({
  page,
}) => {
  const state = await setup(page);
  state.failure = { method: "POST", status: 403 };
  await create(page);
  await expect(
    page.getByRole("heading", { name: "API key access required" }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Create key", exact: true }),
  ).toHaveCount(0);
  expect(state.keys).toHaveLength(1);
});

const dialog = (page) => page.getByRole("dialog");
async function create(page, name = "new-report") {
  await page.getByRole("button", { name: "Create key", exact: true }).click();
  await page.getByRole("textbox", { name: "Key name", exact: true }).fill(name);
  await page.getByRole("button", { name: "Generate key", exact: true }).click();
}

test("create, copy, refresh and empty-body revoke give toast feedback", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await setup(page);
  await create(page);
  await expect(toast(page)).toContainText("API key created");
  await expect(
    dialog(page).getByRole("textbox", { name: "Generated API key" }),
  ).toHaveValue("synthetic-one-time-secret");
  await page.getByRole("button", { name: "Copy key", exact: true }).click();
  await expect(toast(page)).toContainText("Key copied");
  await page.getByRole("button", { name: "Dismiss notification" }).focus();
  await page.keyboard.press("Enter");
  await expect(toast(page)).toHaveCount(0);
  await dialog(page).getByRole("button", { name: "Close", exact: true }).click();
  await expect(page.getByRole("textbox", { name: "Generated API key" })).toHaveCount(0);
  expect(
    await page.evaluate(() => JSON.stringify({ ...localStorage, ...sessionStorage })),
  ).not.toContain("synthetic-one-time-secret");
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(toast(page)).toContainText("up to date");
  await page.getByRole("button", { name: "Revoke key", exact: true }).click();
  await page.getByRole("button", { name: "Confirm revoke", exact: true }).click();
  await expect(toast(page)).toContainText("Revoked new-report");
});

test("server conflicts and date validation stay in the creation dialog and allow retry", async ({
  page,
}) => {
  const state = await setup(page);
  state.keys.push({ ...original, "key-name": "new-report" }); // another session created it after list loading
  await create(page);
  await expect(toast(page)).toContainText("already exists");
  await expect(
    dialog(page).getByRole("textbox", { name: "Key name", exact: true }),
  ).toHaveValue("new-report");
  state.failure = {
    method: "POST",
    status: 400,
    message:
      "expires must be a valid date/time string, for example 2030-01-01T00:00:00Z.",
  };
  await page
    .getByRole("textbox", { name: "Key name", exact: true })
    .fill("retry-report");
  await page.getByRole("button", { name: "Generate key", exact: true }).click();
  await expect(toast(page)).toContainText("valid expiration date");
  await expect(page.getByLabel("Key name", { exact: true })).toHaveValue(
    "retry-report",
  );
  state.failure = null;
  await page.getByRole("button", { name: "Generate key", exact: true }).click();
  await expect(toast(page)).toContainText("API key created");
});

test("rotation reports failures, cancellation and successful completion without revoking early", async ({
  page,
}) => {
  const state = await setup(page);
  await page.getByRole("button", { name: /daily-report.*No expiration/ }).click();
  await page.getByRole("button", { name: "Rotate key", exact: true }).click();
  state.failure = { method: "POST", status: 500 };
  await page.getByRole("button", { name: "Generate replacement" }).click();
  await expect(toast(page)).toContainText(
    "Refresh your keys to check whether the change was saved",
  );
  expect(state.requests).not.toContain("DELETE");
  state.failure = null;
  await page.getByRole("button", { name: "Generate replacement" }).click();
  await expect(toast(page)).toContainText("Replacement created");
  await dialog(page).getByRole("button", { name: "Close", exact: true }).click();
  await dialog(page).getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(toast(page)).toContainText("has not been revoked");
  expect(state.keys).toHaveLength(2);
  await page.getByRole("button", { name: /daily-report No expiration/ }).click();
  await page.getByRole("button", { name: "Rotate key", exact: true }).click();
  await page.getByRole("button", { name: "Generate replacement" }).click();
  await dialog(page).getByRole("button", { name: "Close", exact: true }).click();
  state.failure = { method: "DELETE", status: 500 };
  await page.getByRole("button", { name: "Confirm revoke", exact: true }).click();
  await expect(toast(page)).toContainText("CDA could not complete");
  expect(state.keys.some((key) => key["key-name"] === "daily-report")).toBe(true);
  state.failure = null;
  await page.getByRole("button", { name: "Confirm revoke", exact: true }).click();
  await expect(toast(page)).toContainText("Rotation complete");
});

test("list errors recover, vanished keys are cleared and expired keys warn", async ({
  page,
}) => {
  const state = await setup(page);
  state.failure = { method: "GET", status: 403 };
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "API key access required" }),
  ).toBeVisible();
  state.failure = null;
  state.keys[0].expires = "2020-01-01T00:00:00+0000[Z]";
  await page.getByRole("button", { name: "Check access again", exact: true }).click();
  await expect(toast(page)).toContainText("up to date");
  await page.getByRole("button", { name: /daily-report.*Expired/ }).click();
  await expect(toast(page)).toContainText("has expired");
  state.keys = [];
  await page.getByRole("button", { name: /daily-report.*Expired/ }).click();
  await expect(toast(page)).toContainText("no longer exists");
  await expect(
    page.getByRole("heading", { name: "Key details", exact: true }),
  ).toBeVisible();
});

test("mobile clipboard errors remain dismissible inside the save dialog", async ({
  page,
}) => {
  await setup(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: () => Promise.reject(new Error("Unavailable")) },
    }),
  );
  await create(page);
  await page.getByRole("button", { name: "Copy key", exact: true }).click();
  await expect(toast(page)).toContainText("copy the key manually");
  const bounds = await toast(page).boundingBox();
  expect(bounds.x).toBeGreaterThanOrEqual(0);
  expect(bounds.x + bounds.width).toBeLessThanOrEqual(390);
  await page.getByRole("button", { name: "Dismiss notification" }).click();
  await expect(toast(page)).toHaveCount(0);
  await expect(
    dialog(page).getByRole("textbox", { name: "Generated API key" }),
  ).toBeVisible();
});

test("success feedback expires, errors persist, and long rotation names stay valid", async ({
  page,
}) => {
  await page.clock.install();
  const state = await setup(page);
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(toast(page)).toContainText("up to date");
  await page.clock.fastForward(9000);
  await expect(toast(page)).toHaveCount(0);
  state.failure = { method: "GET", status: 401 };
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(toast(page)).toContainText("sign-in could not be verified");
  await page.clock.fastForward(9000);
  await expect(toast(page)).toContainText("sign-in could not be verified");
  state.failure = null;
  const longName = "a".repeat(64);
  state.keys = [{ ...original, "key-name": longName }];
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page.getByRole("button", { name: new RegExp(longName) }).click();
  await page.getByRole("button", { name: "Rotate key", exact: true }).click();
  expect(
    await page.getByRole("textbox", { name: "Key name", exact: true }).inputValue(),
  ).toHaveLength(64);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  state.keys = [];
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Key details", exact: true }),
  ).toBeVisible();
});

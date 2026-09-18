import { expect, test } from "@playwright/test";

test("published user hooks search across offices, assign membership, and save a preset", async ({
  page,
}) => {
  const deployment = await mockDeployment(page, "pkce");
  const user = { "user-name": "new.staff", roles: { HQ: ["All Users"] } };
  const writes = [];
  const searches = [];
  await page.route("**/cwms-data/offices*", (route) =>
    route.fulfill({
      json: [
        { name: "HQ", "long-name": "Headquarters" },
        { name: "SWT", "long-name": "Tulsa" },
      ],
    }),
  );
  await page.route("**/cwms-data/roles", (route) =>
    route.fulfill({
      json: ["All Users", "CWMS Users", "TS ID Creator", "Data Acquisition Mgr"],
    }),
  );
  await page.route("**/cwms-data/users?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    searches.push(params);
    const users = !params.has("office") || user.roles.SWT ? [user] : [];
    return route.fulfill({ json: { users, total: users.length } });
  });
  await page.route("**/cwms-data/user/new.staff/roles/SWT", (route) => {
    const request = route.request();
    const roles = request.postDataJSON();
    writes.push({
      method: request.method(),
      roles,
      authorization: request.headers().authorization,
    });
    user.roles.SWT = [...new Set([...(user.roles.SWT ?? []), ...roles])];
    return route.fulfill({ status: 204 });
  });
  await page.goto("/cwms-data/user-roles");
  await page.getByRole("button", { name: "Assign office", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog
    .getByRole("textbox", { name: "Username", exact: true })
    .fill("new.staff");
  await dialog.getByRole("button", { name: "Search", exact: true }).click();
  await dialog.getByRole("button", { name: "new.staff", exact: true }).click();
  await expect(dialog.getByRole("combobox")).toHaveValue("SWT");
  await expect(dialog.getByRole("combobox").locator("option")).toHaveCount(1);
  await dialog.getByRole("button", { name: "Assign office", exact: true }).click();
  await expect(dialog).not.toBeVisible();
  await expect(
    page.getByText("Assigned new.staff to SWT. Choose their roles below."),
  ).toBeVisible();
  await page.locator("#role-mode-batchadmin").check();
  await page.getByRole("button", { name: "Save roles", exact: true }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes).toEqual([
    {
      method: "POST",
      roles: ["All Users"],
      authorization: `Bearer ${deployment.token()}`,
    },
    {
      method: "POST",
      roles: ["CWMS Users", "Data Acquisition Mgr", "TS ID Creator"],
      authorization: `Bearer ${deployment.token()}`,
    },
  ]);
  expect(
    searches.some(
      (params) =>
        params.get("username-like") === "new\\.staff" && !params.has("office"),
    ),
  ).toBe(true);
  expect(user.roles.HQ).toEqual(["All Users"]);
});

async function mockDeployment(page, flow) {
  const origin = "http://127.0.0.1:18741";
  const authority = `${flow === "pkce" ? "https://auth.example.test" : origin}/auth/realms/test`;
  let tokenNumber = 0;
  let specRequests = 0;
  const grants = [];
  const requests = [];
  const token = () => `test-access-${tokenNumber}`;
  await page.clock.install();
  if (flow === "pkce") {
    await page.addInitScript(
      ({ authority }) => {
        localStorage.setItem(
          `groundwork-water:keycloak:test:cwms:user:${authority}:cwms`,
          JSON.stringify({
            access_token: "test-access-0",
            refresh_token: "test-refresh-0",
            token_type: "Bearer",
            scope: "openid profile",
            profile: { sub: "test-user" },
            expires_at: Math.floor(Date.now() / 1000) + 300,
          }),
        );
      },
      { authority },
    );
  }
  await page.route("**/.well-known/openid-configuration", (route) =>
    route.fulfill({
      json: {
        issuer: authority,
        authorization_endpoint: `${authority}/protocol/openid-connect/auth`,
        token_endpoint: `${authority}/protocol/openid-connect/token`,
        jwks_uri: `${authority}/protocol/openid-connect/certs`,
      },
    }),
  );
  await page.route("**/protocol/openid-connect/token", (route) => {
    const body = new URLSearchParams(route.request().postData());
    grants.push(Object.fromEntries(body));
    tokenNumber += 1;
    return route.fulfill({
      json: {
        access_token: token(),
        refresh_token: `test-refresh-${tokenNumber}`,
        expires_in: 300,
        token_type: "Bearer",
        scope: "openid profile",
      },
    });
  });
  await page.route("**/protocol/openid-connect/logout", (route) =>
    route.fulfill({ status: 204 }),
  );
  await page.route("**/swagger-docs", (route) => {
    specRequests += 1;
    return route.fulfill({
      json: {
        openapi: "3.0.3",
        info: { title: "Refresh test", version: "1" },
        servers: [{ url: `${origin}/cwms-data` }],
        components: {
          securitySchemes: {
            OpenId: {
              type: "openIdConnect",
              openIdConnectUrl: `${authority}/.well-known/openid-configuration`,
              "x-oidc-client-id": "cwms",
            },
          },
        },
        paths: {
          "/draft": {
            post: {
              summary: "Submit draft",
              requestBody: {
                content: { "application/json": { schema: { type: "object" } } },
              },
              responses: { 200: { description: "OK" } },
            },
          },
        },
      },
    });
  });
  await page.route("**/user/profile*", (route) =>
    route.fulfill({
      json: {
        "user-name": "Test User",
        roles: { SWT: ["CWMS User Admins"] },
      },
    }),
  );
  await page.route("**/cwms-data/draft*", (route) => {
    requests.push({
      authorization: route.request().headers().authorization,
      body: route.request().postData(),
    });
    return route.fulfill({ json: { saved: true } });
  });
  return { grants, requests, token, specRequests: () => specRequests };
}

for (const flow of ["direct-grant", "pkce"]) {
  test(`${flow}: Swagger retains its draft and sends rotated tokens`, async ({
    page,
  }) => {
    const deployment = await mockDeployment(page, flow);
    let navigations = 0;
    page.on("framenavigated", (frame) => {
      if (frame === page.mainFrame()) navigations += 1;
    });
    await page.goto("/cwms-data/swagger-ui");
    await expect(
      page.getByRole("button", { name: "Log out", exact: true }),
    ).toBeVisible();
    await expect.poll(() => deployment.grants.length).toBeGreaterThan(0);
    await page.locator(".opblock-summary").click();
    await page.getByRole("button", { name: "Try it out" }).click();
    const editor = page.locator("textarea.body-param__text");
    const draft = '{"description":"unfinished work"}';
    await editor.fill(draft);
    const editorHandle = await editor.elementHandle();
    const initialSpecRequests = deployment.specRequests();
    const initialNavigations = navigations;
    for (let rotation = 0; rotation < 2; rotation += 1) {
      const previousToken = deployment.token();
      const previousGrantCount = deployment.grants.length;
      await page.clock.fastForward("05:01");
      await expect.poll(() => deployment.token()).not.toBe(previousToken);
      await expect(editor).toHaveValue(draft);
      await expect
        .poll(() => editorHandle.evaluate((node) => node.isConnected))
        .toBe(true);
      await page.getByRole("button", { name: "Execute", exact: true }).click();
      await expect
        .poll(() => deployment.requests.at(-1)?.authorization)
        .toBe(`Bearer ${deployment.token()}`);
      expect(deployment.requests.at(-1).body).toBe(draft);
      expect(
        deployment.grants
          .slice(previousGrantCount)
          .every((grant) => grant.grant_type === "refresh_token"),
      ).toBe(true);
      expect(deployment.specRequests()).toBe(initialSpecRequests);
      expect(navigations).toBe(initialNavigations);
    }
    if (flow === "direct-grant") {
      await page.getByRole("button", { name: "Sign out", exact: true }).click();
      await expect(
        page.getByRole("button", { name: "Sign in", exact: true }),
      ).toBeVisible();
      await expect(editor).toHaveValue(draft);
      const previousRequests = deployment.requests.length;
      await page.getByRole("button", { name: "Execute", exact: true }).click();
      await expect.poll(() => deployment.requests.length).toBe(previousRequests + 1);
      expect(deployment.requests.at(-1).authorization).toBeUndefined();
    }
  });
}

test("the application preserves a user-list form during background refresh", async ({
  page,
}) => {
  const deployment = await mockDeployment(page, "pkce");
  const writes = [];
  await page.route("**/cwms-data/user/list?*", (route) =>
    route.fulfill({ json: { "user-lists": [] } }),
  );
  await page.route("**/cwms-data/user/list", (route) => {
    writes.push({
      authorization: route.request().headers().authorization,
      body: route.request().postDataJSON(),
    });
    return route.fulfill({ json: {} });
  });
  await page.goto("/cwms-data/user-lists");
  await page.getByRole("button", { name: "New list", exact: true }).click();
  await page.getByRole("textbox", { name: "List ID", exact: true }).fill("DRAFT-LIST");
  await page
    .getByRole("textbox", { name: "Description", exact: true })
    .fill("Keep this unfinished description");
  const previousToken = deployment.token();
  await page.clock.fastForward("05:01");
  await expect.poll(() => deployment.token()).not.toBe(previousToken);
  await expect(page.getByRole("textbox", { name: "List ID", exact: true })).toHaveValue(
    "DRAFT-LIST",
  );
  await expect(
    page.getByRole("textbox", { name: "Description", exact: true }),
  ).toHaveValue("Keep this unfinished description");
  await page.getByRole("button", { name: "Create list", exact: true }).click();
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0].authorization).toBe(`Bearer ${deployment.token()}`);
  expect(writes[0].body.description).toBe("Keep this unfinished description");
});

test("an expired OpenID session leaves the Swagger draft available", async ({
  page,
}) => {
  const deployment = await mockDeployment(page, "pkce");
  await page.goto("/cwms-data/swagger-ui");
  await expect(
    page.getByRole("button", { name: "Log out", exact: true }),
  ).toBeVisible();
  await expect.poll(() => deployment.grants.length).toBeGreaterThan(0);
  await page.locator(".opblock-summary").click();
  await page.getByRole("button", { name: "Try it out" }).click();
  const editor = page.locator("textarea.body-param__text");
  await editor.fill('{"description":"recoverable draft"}');
  await page.route("**/protocol/openid-connect/token", (route) =>
    route.fulfill({ status: 400, json: { error: "invalid_grant" } }),
  );
  await page.clock.fastForward("05:01");
  await expect(page.getByRole("button", { name: "Login", exact: true })).toBeVisible();
  await expect(editor).toHaveValue('{"description":"recoverable draft"}');
  await page.getByRole("button", { name: "Execute", exact: true }).click();
  await expect.poll(() => deployment.requests.length).toBe(1);
  expect(deployment.requests[0].authorization).toBeUndefined();
});

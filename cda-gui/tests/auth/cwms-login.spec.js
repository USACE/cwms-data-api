import { expect, test } from "@playwright/test";

const cwmsScheme = {
  type: "apiKey",
  in: "cookie",
  name: "JSESSIONIDSSO",
};

async function mockDeployment(page, schemes = { CwmsAAACacAuth: cwmsScheme }) {
  await page.route("**/swagger-docs", (route) =>
    route.fulfill({
      json: {
        openapi: "3.0.3",
        info: { title: "CWMS authentication test", version: "1" },
        paths: {},
        components: { securitySchemes: schemes },
      },
    }),
  );
  await page.route("**/cwms-data/auth/keys", (route) =>
    route.fulfill({
      status: route.request().headers().cookie?.includes("JSESSIONIDSSO=test")
        ? 200
        : 401,
      json: [],
    }),
  );
  await page.route("**/user/profile*", (route) =>
    route.fulfill({ json: { "user-name": "Test User", roles: {} } }),
  );
  // Simulate the servlet redirect without contacting a real CAC service.
  await page.route("**/CWMSLogin/*", async (route) => {
    const url = new URL(route.request().url());
    await route.fulfill({
      status: 302,
      headers: {
        location: url.searchParams.get("OriginalLocation"),
        "set-cookie":
          url.pathname === "/CWMSLogin/login"
            ? "JSESSIONIDSSO=test; Path=/; HttpOnly"
            : "JSESSIONIDSSO=; Path=/; Max-Age=0; HttpOnly",
      },
    });
  });
}

test("header CWMS login and logout return to the current page", async ({ page }) => {
  await mockDeployment(page);
  await page.goto("/cwms-data/regexp/?office=SWT#login");
  const originalLocation = page.url();

  const loginRequest = page.waitForRequest("**/CWMSLogin/login?*");
  await page.getByRole("button", { name: "Login", exact: true }).click();
  expect(new URL((await loginRequest).url()).searchParams.get("OriginalLocation")).toBe(
    originalLocation,
  );
  await expect(
    page.getByRole("button", { name: "Log out", exact: true }),
  ).toBeVisible();
  await expect(page).toHaveURL(originalLocation);
  await expect(page.getByText("Sign-in unavailable", { exact: true })).toHaveCount(0);

  const logoutRequest = page.waitForRequest("**/CWMSLogin/logout?*");
  await page.getByRole("button", { name: "Log out", exact: true }).click();
  expect(
    new URL((await logoutRequest).url()).searchParams.get("OriginalLocation"),
  ).toBe(originalLocation);
  await expect(page.getByRole("button", { name: "Login", exact: true })).toBeVisible();
  await expect(page).toHaveURL(originalLocation);
});

test("Swagger CWMS login also establishes the header session", async ({ page }) => {
  await mockDeployment(page);
  await page.goto("/cwms-data/swagger-ui");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Log out", exact: true }),
  ).toBeVisible();
  await expect(page).toHaveURL(/\/cwms-data\/swagger-ui$/);
});

test("unsupported authentication still reports sign-in unavailable", async ({
  page,
}) => {
  await mockDeployment(page, {});
  await page.goto("/cwms-data/");
  await expect(page.getByText("Sign-in unavailable", { exact: true })).toBeVisible();
});

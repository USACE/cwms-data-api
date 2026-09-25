import { expect, test } from "@playwright/test";
import { demoProfile, demoRoles } from "./demo-data";

async function openRoles(page) {
  await page.goto("/cwms-data/user-roles");
  await page.getByRole("button", { name: "Log in", exact: true }).last().click();
}

test.beforeEach(async ({ request }) => {
  await request.post("/demo-api/reset");
});

test("defaults to the authorized office with most roles", async ({ page }) => {
  await page.route("**/demo-api/user/profile", (route) =>
    route.fulfill({
      json: {
        ...demoProfile,
        roles: {
          SPK: ["CWMS User Admins"],
          SWT: demoRoles,
          HQ: [...demoRoles, "extra"].filter((role) => role !== "CWMS User Admins"),
        },
      },
    }),
  );
  await openRoles(page);
  await expect(page.locator("#role-office")).toHaveValue("SWT");
});

test("ties use the first listed office; selected office and user survive save and reload", async ({
  page,
}) => {
  await openRoles(page);
  await expect(page.locator("#role-office")).toHaveValue("SPK");
  await page.locator("#role-office").selectOption("SWT");
  await page.getByRole("listitem").filter({ hasText: "ellis.multi" }).click();
  await page.locator("#role-mode-custom").check();
  await page.locator("#role-TS-ID-Creator").check();
  await page.getByRole("button", { name: "Save roles", exact: true }).click();
  await expect(page.getByText("Updated ellis.multi: added 1 role.")).toBeVisible();
  await expect(page.locator("#role-office")).toHaveValue("SWT");
  await expect(
    page.getByRole("heading", { name: "ellis.multi", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Save roles", exact: true }),
  ).toBeDisabled();
  await page.reload();
  // The demo login is in memory; signing in again must restore the stored choice.
  await page.getByRole("button", { name: "Log in", exact: true }).last().click();
  await expect(page.locator("#role-office")).toHaveValue("SWT");
  await expect(
    page.getByRole("heading", { name: "ellis.multi", exact: true }),
  ).toBeVisible();
});

test("stale stored selections fall back to authorized offices and existing users", async ({
  page,
}) => {
  await page.addInitScript(() =>
    localStorage.setItem(
      "cda:user-roles:http://127.0.0.1:18742/demo-api:demo.admin",
      JSON.stringify({ office: "UNKNOWN", userName: "missing.user" }),
    ),
  );
  await openRoles(page);
  await expect(page.locator("#role-office")).toHaveValue("SPK");
  await expect(
    page.getByRole("heading", { name: "ellis.multi", exact: true }),
  ).toBeVisible();
});

test("users without roles are directed to an administrator", async ({ page }) => {
  await page.route("**/demo-api/user/profile", (route) =>
    route.fulfill({ json: { ...demoProfile, roles: {} } }),
  );
  await openRoles(page);
  await expect(
    page.getByText(/Contact an administrator to be assigned roles for your office/),
  ).toBeVisible();
});

test("unavailable local storage does not prevent selecting an office", async ({
  page,
}) => {
  await page.addInitScript(() => {
    Storage.prototype.getItem = () => {
      throw new Error("Storage disabled");
    };
    Storage.prototype.setItem = () => {
      throw new Error("Storage disabled");
    };
  });
  await openRoles(page);
  await expect(page.locator("#role-office")).toHaveValue("SPK");
  await page.locator("#role-office").selectOption("SWT");
  await expect(
    page.getByRole("heading", { name: "devon.tulsa", exact: true }),
  ).toBeVisible();
});

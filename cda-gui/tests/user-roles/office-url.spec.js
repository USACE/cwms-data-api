import { expect, test } from "@playwright/test";
import { demoProfile, demoRoles } from "./demo-data";

async function login(page, path) {
  await page.goto(path);
  await page.getByRole("button", { name: "Log in", exact: true }).last().click();
}

test("office links override storage; dropdown labels are sorted and include HQ", async ({
  page,
}) => {
  await page.addInitScript(() =>
    localStorage.setItem(
      "cda:user-roles:http://127.0.0.1:18742/demo-api:demo.admin",
      JSON.stringify({ office: "SPK", userName: "gray.sacramento" }),
    ),
  );
  await login(page, "/cwms-data/user-roles/swt");
  await expect(page.locator("#role-office")).toHaveValue("SWT");
  await expect(page.locator("#role-office option")).toHaveText([
    "HQ - Headquarters",
    "SPK - Sacramento District",
    "SWT - Tulsa District",
  ]);
  await page.locator("#role-office").selectOption("SPK");
  await expect(page).toHaveURL(/\/user-roles\/spk$/);
  await page.goBack();
  await expect(page.locator("#role-office")).toHaveValue("SWT");
  await page.goForward();
  await expect(page.locator("#role-office")).toHaveValue("SPK");
});

test("HQ is viewable without granting HQ edit permissions", async ({ page }) => {
  await login(page, "/cwms-data/user-roles/hq");
  await expect(page.locator("#role-office")).toHaveValue("HQ");
  await expect(
    page.getByText(
      "You can review HQ users. Contact an HQ administrator to change their roles.",
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "alex.hq", exact: true }),
  ).toBeVisible();
  await expect(page.locator("#role-mode-custom")).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Save roles", exact: true }),
  ).toBeDisabled();
});

test("HQ administrators retain edit access and HQ occurs only once", async ({
  page,
}) => {
  await page.route("**/demo-api/user/profile", (route) =>
    route.fulfill({
      json: {
        ...demoProfile,
        roles: { ...demoProfile.roles, HQ: demoRoles },
      },
    }),
  );
  await login(page, "/cwms-data/user-roles/HQ");
  await expect(page).toHaveURL(/\/user-roles\/hq$/);
  await expect(page.locator('#role-office option[value="HQ"]')).toHaveCount(1);
  await expect(page.locator("#role-mode-custom")).toBeEnabled();
});

test("unknown office URLs fall back to the default office", async ({ page }) => {
  await login(page, "/cwms-data/user-roles/unknown");
  await expect(page).toHaveURL(/\/user-roles\/spk$/);
  await expect(page.locator("#role-office")).toHaveValue("SPK");
});

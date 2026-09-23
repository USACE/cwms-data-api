import { expect, test } from "@playwright/test";
import { demoProfile } from "./demo-data";

test.beforeEach(async ({ request }) => {
  await request.post("/demo-api/reset");
});

async function openRoles(page) {
  await page.goto("/cwms-data/user-roles");
  const login = page.getByRole("button", { name: "Log in", exact: true });
  await login.last().click();
  await expect(
    page.getByRole("button", { name: "Onboard users", exact: true }),
  ).toBeVisible();
}

test("the displayed office matches the initial save destination and follows office changes", async ({
  page,
  request,
}) => {
  await openRoles(page);
  await expect(page.getByRole("combobox")).toHaveValue("SPK");
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: /alex.hq/ }).click();
  await expect(dialog.locator("#onboarding-office")).toHaveValue("SPK");
  await dialog
    .getByRole("button", { name: "Assign office and roles", exact: true })
    .click();
  await expect(
    dialog.getByText("Assigned alex.hq to SPK. Existing office roles were kept."),
  ).toBeVisible();
  const data = await (await request.get("/demo-api/users")).json();
  expect(data.users.find((user) => user["user-name"] === "alex.hq").roles).toEqual({
    HQ: ["All Users", "CWMS Users"],
    SPK: ["All Users", "CWMS Users"],
  });
  await dialog.getByRole("button", { name: "Close", exact: true }).click();
  await page.getByRole("combobox").selectOption("SWT");
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  await dialog.getByRole("button", { name: /blair.unassigned/ }).click();
  await expect(dialog.locator("#onboarding-office")).toHaveValue("SWT");
});

test("onboard HQ and unassigned users, preserve HQ, and refresh the office list", async ({
  page,
  request,
}) => {
  await openRoles(page);
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("3 users to onboard", { exact: true })).toBeVisible();
  await expect(dialog.getByRole("button", { name: /alex.hq/ })).toBeVisible();
  await expect(dialog.getByRole("button", { name: /blair.unassigned/ })).toBeVisible();
  await expect(dialog.getByRole("button", { name: /casey.baseline/ })).toBeVisible();
  await expect(
    dialog.getByRole("button", { name: /devon|ellis|frankie|gray/ }),
  ).toHaveCount(0);
  await dialog.getByRole("button", { name: /alex.hq/ }).click();
  await dialog.locator("#onboarding-office").selectOption("SWT");
  await dialog.getByLabel("Roles to assign").selectOption("readwrite");
  await dialog
    .getByRole("button", { name: "Assign office and roles", exact: true })
    .click();
  await expect(
    dialog.getByText("Assigned alex.hq to SWT. Existing office roles were kept."),
  ).toBeVisible();
  await expect(dialog.getByRole("button", { name: /alex.hq/ })).toHaveCount(0);
  await dialog.getByRole("button", { name: /blair.unassigned/ }).click();
  await dialog.locator("#onboarding-office").selectOption("SPK");
  await dialog.getByLabel("Roles to assign").selectOption("custom");
  await dialog.getByLabel("CWMS Users", { exact: true }).check();
  await dialog.getByLabel("Data Acquisition Mgr", { exact: true }).check();
  await expect(dialog.getByLabel("All Users", { exact: true })).toBeDisabled();
  await dialog
    .getByRole("button", { name: "Assign office and roles", exact: true })
    .click();
  await expect(
    dialog.getByText(
      "Assigned blair.unassigned to SPK. Existing office roles were kept.",
    ),
  ).toBeVisible();
  const data = await (await request.get("/demo-api/users")).json();
  expect(data.users.find((user) => user["user-name"] === "alex.hq").roles).toEqual({
    HQ: ["All Users", "CWMS Users"],
    SWT: ["All Users", "CWMS Users", "TS ID Creator"],
  });
  expect(
    data.users.find((user) => user["user-name"] === "blair.unassigned").roles.SPK,
  ).toEqual(["All Users", "CWMS Users", "Data Acquisition Mgr"]);
  await dialog.getByRole("button", { name: "Close", exact: true }).click();
  await page.getByRole("combobox").selectOption("SWT");
  await expect(page.getByRole("listitem").filter({ hasText: "alex.hq" })).toBeVisible();
  await expect(
    page.getByRole("listitem").filter({ hasText: "devon.tulsa" }),
  ).toBeVisible();
});

test("failed saves keep the selection and allow retry without claiming success", async ({
  page,
}) => {
  await openRoles(page);
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: /alex.hq/ }).click();
  await page.route("**/demo-api/user/alex.hq/roles/*", (route) =>
    route.fulfill({ status: 403, json: { message: "Office access denied" } }),
  );
  await dialog
    .getByRole("button", { name: "Assign office and roles", exact: true })
    .click();
  await expect(
    dialog.getByText(/Refresh users to review any roles already saved/),
  ).toBeVisible();
  await expect(dialog.getByRole("button", { name: /alex.hq/ })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  await expect(dialog.getByText(/^Assigned alex/)).toHaveCount(0);
  await page.unroute("**/demo-api/user/alex.hq/roles/*");
  await dialog
    .getByRole("button", { name: "Assign office and roles", exact: true })
    .click();
  await expect(dialog.getByText(/^Assigned alex/)).toBeVisible();
});

test("missing PD hides office assignment while retaining the office role editor", async ({
  page,
}) => {
  await page.route("**/demo-api/user/profile*", (route) =>
    route.fulfill({ json: { ...demoProfile, roles: { SWT: ["CWMS User Admins"] } } }),
  );
  await page.goto("/cwms-data/user-roles");
  await page.getByRole("button", { name: "Log in", exact: true }).last().click();
  await expect(page.getByText("Office users", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Onboard users", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Assign office", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByText(/To assign users to an office, you must have both/),
  ).toBeVisible();
});

test("all pages are filtered across offices and an empty search is explained", async ({
  page,
}) => {
  const pages = [];
  await page.route("**/demo-api/users?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.has("office")) return route.continue();
    pages.push(params.get("page"));
    return route.fulfill({
      json: params.has("page")
        ? { users: [{ "user-name": "last.page.user", roles: {} }], total: 2 }
        : {
            users: [
              {
                "user-name": "already.assigned",
                roles: { HQ: ["CWMS Users"], SWT: ["All Users"] },
              },
            ],
            total: 2,
            "next-page": "second",
          },
    });
  });
  await openRoles(page);
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("button", { name: /last.page.user/ })).toBeVisible();
  await expect(dialog.getByRole("button", { name: /already.assigned/ })).toHaveCount(0);
  expect(pages).toEqual([null, "second"]);
  await dialog.getByRole("textbox", { name: "Users to onboard" }).fill("no-match");
  await expect(dialog.getByText("No matching users.")).toBeVisible();
});

test("mobile onboarding fits the viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openRoles(page);
  await page.getByRole("button", { name: "Onboard users", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: /alex.hq/ }).click();
  await expect(
    dialog.getByRole("button", { name: "Assign office and roles", exact: true }),
  ).toBeEnabled();
  expect(
    await dialog.evaluate((element) => element.scrollWidth <= element.clientWidth),
  ).toBe(true);
});

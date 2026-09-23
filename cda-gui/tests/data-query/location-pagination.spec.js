import { expect, test } from "@playwright/test";

async function searchLocations(page, pageCount) {
  const requests = [];
  await page.route("**/cwms-data/offices*", (route) =>
    route.fulfill({ json: [{ name: "SWT", "long-name": "Tulsa" }] }),
  );
  await page.route("**/cwms-data/swagger-docs*", (route) =>
    route.fulfill({ json: { components: { securitySchemes: {} } } }),
  );
  await page.route("**/cwms-data/catalog/LOCATIONS*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    requests.push(Object.fromEntries(params));
    const pageNumber = parseInt(params.get("page")?.split("/")[1] || "1", 10);
    return route.fulfill({
      json: {
        entries: [{ name: `TEST-${pageNumber}`, office: "SWT", active: true }],
        "next-page": pageNumber < pageCount ? `cursor/${pageNumber + 1}+=` : null,
      },
    });
  });
  await page.goto("/cwms-data/data-query");
  await page.getByRole("button", { name: "Guided", exact: true }).click();
  await page
    .locator("select")
    .filter({ has: page.locator('option[value="PROJECT"]') })
    .selectOption("PROJECT");
  await page.getByPlaceholder("Search locations by name or ID").fill("TEST");
  return requests;
}

for (const pageCount of [1, 3, 11]) {
  test(`guided locations load ${Math.min(pageCount, 10)} of ${pageCount} catalog pages`, async ({
    page,
  }) => {
    const requests = await searchLocations(page, pageCount);
    const expectedCount = Math.min(pageCount, 10);
    await expect(page.getByRole("option")).toHaveCount(expectedCount);
    for (let number = 1; number <= expectedCount; number += 1) {
      await expect(
        page.getByRole("option", {
          name: `SWT / TEST-${number} SWT / TEST-${number}`,
          exact: true,
        }),
      ).toBeVisible();
    }
    expect(requests).toHaveLength(expectedCount);
    requests.forEach((request, index) => {
      expect(request).toMatchObject({
        "page-size": "2000",
        like: "*TEST*",
        "location-kind-like": "PROJECT",
      });
      expect(request.page).toBe(index === 0 ? undefined : `cursor/${index + 1}+=`);
    });
  });
}

import { expect, test } from "@playwright/test";

test("environment is visible in the shared banner before login at desktop and mobile sizes", async ({
  page,
}) => {
  await page.route("**/swagger-docs", (route) =>
    route.fulfill({ json: { openapi: "3.0.3", paths: {} } }),
  );
  for (const path of ["", "timestamps"]) {
    await page.goto(`/cwms-data/${path}`);
    const header = page.getByRole("banner").first();
    const badge = header.getByLabel("Environment: Test", { exact: true });
    for (const width of [1440, 768, 390, 320]) {
      await page.setViewportSize({ width, height: 900 });
      await expect(badge).toBeVisible();
      await expect(badge).toHaveText("Test");
      await expect(
        header.getByText("US Army Corps of Engineers", { exact: true }),
      ).toBeVisible();
      const bounds = await badge.boundingBox();
      expect(bounds.x).toBeGreaterThanOrEqual(0);
      expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);
    }
  }
});

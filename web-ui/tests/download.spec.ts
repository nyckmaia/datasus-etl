import { test, expect } from "@playwright/test";

test("download wizard advances from step 1 to step 2", async ({ page }) => {
  await page.goto("/download/step-1");

  await expect(
    page.getByRole("heading", { name: /Choose a subsystem/i }),
  ).toBeVisible();

  // Pick a subsystem (SIHSUS preferred, fallback to first available card).
  const sihsus = page.getByRole("button", { name: /SIHSUS/i }).first();
  if (await sihsus.count()) {
    await sihsus.click();
  } else {
    await page.locator('button:has(span.font-mono)').first().click();
  }

  await page.getByRole("button", { name: /Continue/i }).click();

  await expect(
    page.getByRole("heading", { name: /Choose scope/i }),
  ).toBeVisible();
});

import { test, expect } from "@playwright/test";
import path from "node:path";

test("settings page saves the data directory", async ({ page }) => {
  const tmpDir = path.resolve(__dirname, "./.tmp-data-saved");

  await page.goto("/settings");

  await expect(
    page.getByRole("heading", { name: "Settings", level: 1 }),
  ).toBeVisible();

  const input = page.getByLabel("Absolute path");
  await input.fill(tmpDir);
  await page.getByRole("button", { name: /^Save$/ }).click();

  await expect(
    page.getByText(/Data directory updated/i),
  ).toBeVisible({ timeout: 10_000 });
});

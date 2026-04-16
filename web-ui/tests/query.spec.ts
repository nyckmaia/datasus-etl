import { test, expect } from "@playwright/test";

test("query page runs a basic SELECT", async ({ page }) => {
  await page.goto("/query");

  await expect(page.getByRole("heading", { name: "Query", level: 1 })).toBeVisible();

  // Wait for Monaco to be ready.
  const editor = page.locator(".monaco-editor").first();
  await expect(editor).toBeVisible({ timeout: 15_000 });

  // Select all and type a simple statement.
  await editor.click();
  await page.keyboard.press("Control+A");
  await page.keyboard.type("SELECT 1 AS n");

  await page.getByRole("button", { name: /^Run$/ }).click();

  // The result table should show one row with value 1.
  await expect(page.getByText("1 rows")).toBeVisible({ timeout: 10_000 });
  await expect(page.locator("td").filter({ hasText: /^1$/ }).first()).toBeVisible();
});

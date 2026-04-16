import { test, expect } from "@playwright/test";

test("dashboard renders heading and either empty state or subsystems", async ({
  page,
}) => {
  await page.goto("/");

  await expect(
    page.getByRole("heading", { name: "Dashboard", level: 1 }),
  ).toBeVisible();

  // Either the empty state or at least one subsystem card is rendered.
  const emptyHeading = page.getByRole("heading", { name: /No datasets yet/i });
  const subsystemsHeading = page.getByText(/Subsystems/i).first();

  await expect(emptyHeading.or(subsystemsHeading)).toBeVisible({
    timeout: 15_000,
  });
});

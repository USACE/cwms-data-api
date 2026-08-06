import { expect } from "storybook/test";

import Home from "./Home";

const meta = {
  title: "Pages/Home",
  component: Home,
};

export default meta;

export const LandingPage = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Introduction" })).toBeVisible();
    await expect(
      canvas.getByText(
        "Welcome to the US Army Corps of Engineers Corps Water Management System Data API.",
      ),
    ).toBeVisible();
    await expect(canvas.getAllByRole("link", { name: "Swagger UI" })[0]).toBeVisible();
    await expect(canvas.getByRole("link", { name: "Data Query Tool" })).toBeVisible();
    await expect(
      canvas.getByRole("link", { name: "Regular Expressions" }),
    ).toBeVisible();
  },
};

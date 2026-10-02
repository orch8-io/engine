import { afterEach } from "vitest";
import { defineOrch8Elements } from "../src/index.js";
import { clearThemeCache } from "../src/theme.js";

defineOrch8Elements();

afterEach(() => {
  document.body.replaceChildren();
  clearThemeCache();
});

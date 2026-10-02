import "server-only";
import { cookies } from "next/headers";
import { resolveUser, USER_COOKIE, type DemoUser } from "./users";

/** STUB: returns the "logged-in" demo user picked in the header switcher. */
export async function getCurrentUser(): Promise<DemoUser> {
  const jar = await cookies();
  return resolveUser(jar.get(USER_COOKIE)?.value);
}

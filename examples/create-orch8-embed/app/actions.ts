"use server";

import { cookies } from "next/headers";
import { revalidatePath } from "next/cache";
import { DEMO_USERS, USER_COOKIE } from "@/lib/users";

/** STUB AUTH: "log in" as another demo user. */
export async function switchUser(formData: FormData): Promise<void> {
  const id = String(formData.get("user") ?? "");
  if (!DEMO_USERS.some((u) => u.id === id)) return;
  (await cookies()).set(USER_COOKIE, id, { httpOnly: true, sameSite: "lax", path: "/" });
  revalidatePath("/", "layout");
}

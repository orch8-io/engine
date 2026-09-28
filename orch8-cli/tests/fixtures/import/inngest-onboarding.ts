import { inngest } from "./client";

export const onboarding = inngest.createFunction(
  {
    id: "user-onboarding",
    retries: 2,
    concurrency: { limit: 10, key: "event.data.accountId" },
    cancelOn: [{ event: "app/user.deleted", match: "data.userId" }],
  },
  { event: "app/user.signup" },
  async ({ event, step }) => {
    const user = await step.run("fetch-user", async () => {
      return db.users.find(event.data.userId);
    });

    await step.run("send-welcome", async () => {
      await email.send(user.email, "welcome");
    });

    await step.sleep("wait-a-day", "1d");

    const activated = await step.waitForEvent("wait-for-activation", {
      event: "app/user.activated",
      match: "data.userId",
      timeout: "3d",
    });

    if (user.plan === "pro" && event.data.source !== "import") {
      await Promise.all([
        step.run("provision-workspace", async () => provision(user)),
        step.run("notify-sales", async () => slack.post("#sales", user.email)),
      ]);
    } else {
      await step.run("send-free-tips", async () => email.send(user.email, "tips"));
    }

    for (const team of event.data.teams) {
      await step.run("invite-team", async () => invite(team));
    }

    if (Math.random() > 0.5) {
      await step.run("lottery", async () => prize(user));
    }

    await step.sendEvent("emit-onboarded", {
      name: "app/user.onboarded",
      data: { userId: event.data.userId },
    });

    await step.invoke("score-lead", { function: scoreLead, data: { email: user.email } });
    await step.waitForSignal("custom", { signal: "x", timeout: "1h" });
    return { done: true };
  },
);

export const nightly = inngest.createFunction(
  { id: "nightly-report" },
  { cron: "TZ=Europe/Paris 0 6 * * *" },
  async ({ step }) => {
    await step.run("build-report", async () => build());
  },
);

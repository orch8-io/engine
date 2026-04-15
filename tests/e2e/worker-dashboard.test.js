import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

describe("Worker Dashboard Endpoints", () => {
    let server;

    before(async () => {
        server = await startServer();
    });

    after(async () => {
        await stopServer(server);
    });

    it("GET /workers/tasks returns empty list when no tasks", async () => {
        const tasks = await client.listWorkerTasks();
        assert.ok(Array.isArray(tasks));
    });

    it("GET /workers/tasks/stats returns valid structure", async () => {
        const stats = await client.workerTaskStats();
        assert.ok(stats.by_state !== undefined);
        assert.ok(stats.by_handler !== undefined);
        assert.ok(Array.isArray(stats.active_workers));
    });

    it("GET /workers/tasks lists tasks after dispatch", async () => {
        const seq = testSequence("dash-list", [
            step("s1", "dash_handler", { x: 1 }),
        ]);
        await client.createSequence(seq);
        await client.createInstance({
            sequence_id: seq.id,
            tenant_id: "test",
            namespace: "default",
        });

        let tasks = [];
        for (let i = 0; i < 30; i++) {
            tasks = await client.listWorkerTasks({ handler_name: "dash_handler" });
            if (tasks.length > 0) break;
            await new Promise((r) => setTimeout(r, 200));
        }
        assert.ok(tasks.length > 0, "Expected at least one worker task");
        assert.equal(tasks[0].handler_name, "dash_handler");
    });

    it("GET /workers/tasks filters by state", async () => {
        const pending = await client.listWorkerTasks({ state: "pending", handler_name: "dash_handler" });
        for (const t of pending) {
            assert.equal(t.state, "pending");
        }
    });

    it("GET /workers/tasks/stats reflects dispatched tasks", async () => {
        const stats = await client.workerTaskStats();
        assert.ok(
            stats.by_handler["dash_handler"] !== undefined,
            "Expected dash_handler in stats"
        );
    });
});

// Temporal workflow: three sequential activity calls (the benchmark workload).
// Kept CommonJS so the Temporal workflow bundler loads it without ESM caveats.
const { proxyActivities } = require("@temporalio/workflow");

const { benchActivity } = proxyActivities({ startToCloseTimeout: "30 seconds" });

async function bench3Step(workflowKey) {
  for (const step of ["s1", "s2", "s3"]) {
    await benchActivity(workflowKey, step);
  }
}

module.exports = { bench3Step };

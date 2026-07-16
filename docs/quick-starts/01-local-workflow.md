# Level 1: Run one local workflow

In this level you will run a complete workflow in one process. There is no HTTP
server, API key, or external database yet. The goal is to learn the four nouns
that every later guide uses: sequence, block, handler, and instance.

**Starting point:** the `orch8` binary is available in `PATH`.

**Result:** a three-step workflow completes locally, including a durable-style
delay that is fast-forwarded by the development clock.

## 1. Create a workspace

From the repository root:

```bash
mkdir -p quickstart-work/level-1
cd quickstart-work/level-1
```

Create `sequence.json` with this definition:

```json
{
  "tenant_id": "demo",
  "namespace": "default",
  "name": "hello-local",
  "version": 1,
  "blocks": [
    {
      "type": "step",
      "id": "greet",
      "handler": "log",
      "params": {
        "message": "Hello from your first Orch8 workflow"
      }
    },
    {
      "type": "step",
      "id": "wait_a_moment",
      "handler": "noop",
      "delay": {
        "duration": 3000
      }
    },
    {
      "type": "step",
      "id": "finish",
      "handler": "log",
      "params": {
        "message": "The workflow completed"
      }
    }
  ]
}
```

The development runner supplies the sequence ID, timestamp, tenant, and a new
immutable version for the local session. That is why this authoring file can
stay small.

## 2. Run exactly one instance

```bash
orch8 dev . --skip-timers --once
```

`--once` exits after the instance reaches a terminal state. `--skip-timers`
uses a virtual clock: the three-second delay keeps its scheduling semantics but
does not make the tutorial wait three real seconds.

Look for progress for `greet`, `wait_a_moment`, and `finish`, followed by a
completed terminal state. The exact IDs and timestamps vary on every run.

## 3. Read the definition as the engine does

The top-level object is a **sequence**. It has a stable name and a version. Its
`blocks` array is evaluated from top to bottom.

Each object here is a step **block**. A step calls one **handler**:

| Block ID | Handler | Meaning |
|---|---|---|
| `greet` | `log` | Run the built-in log handler |
| `wait_a_moment` | `noop` | Wait until scheduled, then succeed with `{}` |
| `finish` | `log` | Run another built-in handler |

The development runner creates one **instance** of this sequence. A second run
creates another instance; it does not resume or overwrite the first one.

The block `id` is more than a label. Orch8 uses it to track progress, persist
outputs, report failures, and resolve downstream references. Every block ID in
a sequence must therefore be unique, including blocks nested inside routers or
parallel branches.

## 4. See validation fail safely

Make a temporary invalid copy with duplicate block IDs:

```bash
cp sequence.json invalid-sequence.json
sed -i.bak 's/"id": "finish"/"id": "greet"/' invalid-sequence.json
orch8 dev invalid-sequence.json --once
```

The command must reject the definition before executing it. Remove the
temporary files afterward:

```bash
rm invalid-sequence.json invalid-sequence.json.bak
```

This is the first important safety boundary: invalid workflow structure is a
definition error, not a partially executed instance.

## 5. Try normal development mode

Run without `--once`:

```bash
orch8 dev . --skip-timers
```

While it is running, edit the final message and save `sequence.json`. The
runner notices the file change, publishes a new local version, and starts a new
instance. Press `Ctrl-C` when finished.

This hot-reload loop is for workflow authoring. It does not replace the durable
server introduced in Level 3.

## Checkpoint

You are ready for Level 2 when all of these are true:

- `orch8 dev . --skip-timers --once` exits successfully.
- You can identify the sequence, its three blocks, their handlers, and the one
  instance created by the command.
- You understand that `delay.duration` is milliseconds and `--skip-timers`
  changes the test clock, not the workflow definition.
- You saw structural validation reject the duplicate block ID before running.

Next: [Pass data and choose a route](02-data-and-routing.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| `orch8: command not found` | The release binary directory is not in `PATH` | Return to the repository root and export `PATH="$PWD/target/release:$PATH"` |
| `no sequence.json found` | The command is running in the wrong directory | Run it in `quickstart-work/level-1`, or pass the JSON file path |
| `unknown handler` and the run stalls | A handler name is not built in | Use `log` or `noop` exactly as shown; external handlers arrive in Level 4 |
| The delay takes real time | `--skip-timers` was omitted | Stop the run and include the flag |


import express from "express";
import Docker from "dockerode";
import fs from "node:fs/promises";
import path from "node:path";
import pino from "pino";
import { randomUUID } from "node:crypto";
import { PassThrough } from "node:stream";

const logger = pino({ level: process.env.LOG_LEVEL || "info" });
const app = express();
app.use(express.json({ limit: "2mb" }));

const port = Number(process.env.MCP_BRIDGE_PORT || 4000);
const storagePath = process.env.MCP_TASK_STORAGE || "/data/tasks.json";
const workerPrefix = process.env.WORKER_PREFIX || "dockweave-worker-";
const dockerInspection = String(process.env.MCP_DOCKER_INSPECTION || "true") === "true";
const maxLogBytes = Number(process.env.MCP_MAX_LOG_BYTES || 120000);
const maxDiffBytes = Number(process.env.MCP_MAX_DIFF_BYTES || 200000);

const docker = new Docker({ socketPath: "/var/run/docker.sock" });
const state = {
  tasks: new Map(),
  events: [],
  inFlight: new Set()
};

const TOOL_DEFINITIONS = [
  {
    name: "list_agents",
    description: "List available worker containers and their runtime state.",
    inputSchema: { type: "object", additionalProperties: false, properties: {} }
  },
  {
    name: "inspect_agent",
    description: "Inspect one worker container by name.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["name"],
      properties: { name: { type: "string" } }
    }
  },
  {
    name: "submit_task",
    description: "Create and dispatch a single-agent git task with automated review artifact collection.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["prompt", "repo"],
      properties: {
        prompt: { type: "string" },
        repo: { type: "string" },
        branch: { type: "string" },
        targetAgent: { type: "string" },
        command: { type: "string" },
        testCommand: { type: "string" },
        baseDir: { type: "string" }
      }
    }
  },
  {
    name: "submit_task_multi",
    description: "Create and dispatch a fanout task routed to multiple workers.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["prompt", "repo"],
      properties: {
        prompt: { type: "string" },
        repo: { type: "string" },
        branch: { type: "string" },
        fanout: { type: "integer", minimum: 1 },
        command: { type: "string" },
        testCommand: { type: "string" },
        baseDir: { type: "string" }
      }
    }
  },
  {
    name: "task_status",
    description: "Return one task by task ID.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["taskId"],
      properties: { taskId: { type: "string" } }
    }
  },
  {
    name: "list_tasks",
    description: "List all tasks sorted by creation time descending.",
    inputSchema: { type: "object", additionalProperties: false, properties: {} }
  },
  {
    name: "task_diff",
    description: "Return worker-generated diff artifact for a task.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["taskId"],
      properties: { taskId: { type: "string" } }
    }
  },
  {
    name: "approve_action",
    description: "Record approval for a pending task action.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      required: ["taskId", "action"],
      properties: {
        taskId: { type: "string" },
        action: { type: "string" },
        approvedBy: { type: "string" }
      }
    }
  }
];

function addEvent(type, payload = {}) {
  const event = {
    id: randomUUID(),
    type,
    at: new Date().toISOString(),
    payload
  };
  state.events.push(event);
  if (state.events.length > 500) {
    state.events.shift();
  }
  return event;
}

function shQuote(value) {
  return `'${String(value).replace(/'/g, `'"'"'`)}'`;
}

function clampTail(value, maxBytes) {
  if (!value) {
    return "";
  }
  return value.length > maxBytes ? value.slice(-maxBytes) : value;
}

function repoNameFromUrl(repoUrl) {
  const last = String(repoUrl || "repo").trim().split("/").pop() || "repo";
  return last.endsWith(".git") ? last.slice(0, -4) : last;
}

async function ensureStorage() {
  const dir = path.dirname(storagePath);
  await fs.mkdir(dir, { recursive: true });
  try {
    const raw = await fs.readFile(storagePath, "utf-8");
    const parsed = JSON.parse(raw);
    for (const task of parsed.tasks || []) {
      state.tasks.set(task.id, task);
    }
    logger.info({ count: state.tasks.size }, "loaded task storage");
  } catch (error) {
    if (error.code !== "ENOENT") {
      logger.warn({ err: error }, "unable to preload task storage");
    }
    await persistState();
  }
}

async function persistState() {
  const payload = {
    updatedAt: new Date().toISOString(),
    tasks: Array.from(state.tasks.values())
  };
  await fs.writeFile(storagePath, JSON.stringify(payload, null, 2));
}

function asText(result) {
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(result, null, 2)
      }
    ]
  };
}

async function listWorkers() {
  if (!dockerInspection) {
    return [];
  }

  const containers = await docker.listContainers({ all: true });
  return containers
    .filter((c) => c.Names?.some((name) => name.includes(workerPrefix)))
    .map((c) => ({
      id: c.Id,
      name: c.Names?.[0]?.replace(/^\//, "") || "unknown",
      image: c.Image,
      state: c.State,
      status: c.Status
    }));
}

function getTask(taskId) {
  return state.tasks.get(taskId) || null;
}

function pickWorker(preferred, workers) {
  if (preferred) {
    return workers.find((w) => w.name === preferred) || null;
  }
  return workers.find((w) => w.state === "running") || workers[0] || null;
}

async function dockerExec(containerName, script) {
  const container = docker.getContainer(containerName);
  const exec = await container.exec({
    Cmd: ["bash", "-lc", script],
    AttachStdout: true,
    AttachStderr: true,
    Tty: false
  });

  const stream = await exec.start({ hijack: true, stdin: false });
  const stdout = new PassThrough();
  const stderr = new PassThrough();

  container.modem.demuxStream(stream, stdout, stderr);

  let stdoutRaw = "";
  let stderrRaw = "";

  stdout.on("data", (chunk) => {
    stdoutRaw += chunk.toString("utf-8");
  });

  stderr.on("data", (chunk) => {
    stderrRaw += chunk.toString("utf-8");
  });

  await new Promise((resolve, reject) => {
    stream.on("end", resolve);
    stream.on("error", reject);
  });

  const inspect = await exec.inspect();
  return {
    exitCode: inspect.ExitCode,
    stdout: stdoutRaw,
    stderr: stderrRaw
  };
}

function buildWorkerScript(task) {
  const baseDir = task.input.baseDir || "/workspace/tasks";
  const repo = task.input.repo;
  const branch = task.input.branch || `dockweave/${task.id.slice(0, 8)}`;
  const command = task.input.command || "echo 'No command supplied; repository prepared for manual/agent edits.'";
  const testCommand = task.input.testCommand || "echo 'No testCommand supplied'";
  const repoName = repoNameFromUrl(repo);
  const runDir = `${baseDir}/${task.id}`;

  return `set -euo pipefail
mkdir -p ${shQuote(runDir)}
cd ${shQuote(runDir)}
if [ ! -d ${shQuote(repoName)}/.git ]; then
  git clone ${shQuote(repo)} ${shQuote(repoName)}
fi
cd ${shQuote(repoName)}
git fetch --all --prune || true
git checkout -B ${shQuote(branch)}
${command}
TEST_EXIT=0
${testCommand} || TEST_EXIT=$?
printf "\n__DOCKWEAVE_STATUS__\n"
git status --short || true
printf "\n__DOCKWEAVE_DIFF__\n"
git diff --patch || true
printf "\n__DOCKWEAVE_TEST_EXIT__%s\n" "$TEST_EXIT"
exit 0`;
}

function extractDiffBlock(stdout) {
  const marker = "__DOCKWEAVE_DIFF__";
  const statusMarker = "__DOCKWEAVE_STATUS__";
  const testMarker = "__DOCKWEAVE_TEST_EXIT__";

  const statusIdx = stdout.indexOf(statusMarker);
  const diffIdx = stdout.indexOf(marker);
  const testIdx = stdout.indexOf(testMarker);

  const statusText = statusIdx >= 0 && diffIdx > statusIdx ? stdout.slice(statusIdx + statusMarker.length, diffIdx).trim() : "";
  const diffText = diffIdx >= 0 ? stdout.slice(diffIdx + marker.length, testIdx >= 0 ? testIdx : undefined).trim() : "";
  const testText = testIdx >= 0 ? stdout.slice(testIdx + testMarker.length).trim() : "";

  return {
    statusText,
    diffText,
    testExitCode: testText ? Number.parseInt(testText, 10) : null
  };
}

async function runTaskInWorker(task) {
  if (task.assignedAgents.length === 0) {
    task.status = "pending_no_worker";
    task.finishedAt = new Date().toISOString();
    await persistState();
    return;
  }

  const workerName = task.assignedAgents[0];
  task.status = "running";
  task.startedAt = new Date().toISOString();
  await persistState();

  try {
    const script = buildWorkerScript(task);
    const run = await dockerExec(workerName, script);
    const parsed = extractDiffBlock(run.stdout);

    task.status = run.exitCode === 0 ? "completed" : "failed";
    task.finishedAt = new Date().toISOString();
    task.execution = {
      worker: workerName,
      exitCode: run.exitCode,
      testExitCode: parsed.testExitCode,
      stdoutTail: clampTail(run.stdout, maxLogBytes),
      stderrTail: clampTail(run.stderr, maxLogBytes)
    };

    task.review = {
      summary:
        run.exitCode === 0
          ? "Worker execution finished; review git status and diff before approving commit/push/PR."
          : "Worker execution failed; inspect stderr/stdout before retrying.",
      gitStatus: parsed.statusText,
      testExitCode: parsed.testExitCode
    };

    task.diff = {
      hasChanges: Boolean(parsed.diffText),
      patch: clampTail(parsed.diffText, maxDiffBytes),
      generatedAt: new Date().toISOString()
    };

    addEvent("task.executed", {
      taskId: task.id,
      worker: workerName,
      exitCode: run.exitCode,
      testExitCode: parsed.testExitCode,
      hasDiff: task.diff.hasChanges
    });
  } catch (error) {
    task.status = "failed";
    task.finishedAt = new Date().toISOString();
    task.execution = {
      worker: workerName,
      exitCode: 1,
      stderrTail: String(error.message || error)
    };
    task.review = {
      summary: "Bridge failed while dispatching worker command.",
      gitStatus: null,
      testExitCode: null
    };
    task.diff = {
      hasChanges: false,
      patch: "",
      generatedAt: new Date().toISOString()
    };

    addEvent("task.execution_failed", { taskId: task.id, worker: workerName, error: String(error.message || error) });
  }

  await persistState();
}

function dispatchTask(task) {
  if (state.inFlight.has(task.id)) {
    return;
  }

  state.inFlight.add(task.id);
  runTaskInWorker(task)
    .catch((error) => {
      logger.error({ err: error, taskId: task.id }, "task dispatch crashed");
    })
    .finally(() => {
      state.inFlight.delete(task.id);
    });
}

async function createTask({ kind, input, targetAgent, fanout = 1 }) {
  const workers = await listWorkers();
  const selected = [];

  if (workers.length > 0) {
    if (kind === "multi") {
      selected.push(...workers.slice(0, fanout));
    } else {
      const chosen = pickWorker(targetAgent, workers);
      if (chosen) {
        selected.push(chosen);
      }
    }
  }

  const task = {
    id: randomUUID(),
    kind,
    createdAt: new Date().toISOString(),
    status: selected.length > 0 ? "queued" : "pending_no_worker",
    targetAgent: targetAgent || null,
    assignedAgents: selected.map((w) => w.name),
    input,
    approvals: [],
    diagnostics: {
      workerCount: workers.length,
      dockerInspection,
      workerPrefix
    },
    review: null,
    diff: null,
    execution: null
  };

  state.tasks.set(task.id, task);
  addEvent("task.created", {
    taskId: task.id,
    kind: task.kind,
    assignedAgents: task.assignedAgents
  });
  await persistState();

  if (kind === "multi" && task.assignedAgents.length > 1) {
    // Baseline fanout: execute on first selected worker; full aggregation can be added incrementally.
    task.assignedAgents = [task.assignedAgents[0]];
  }

  dispatchTask(task);
  return task;
}

async function approveTask({ taskId, action, approvedBy }) {
  const task = getTask(taskId);
  if (!task) {
    return null;
  }

  task.approvals.push({
    action,
    approvedBy: approvedBy || "local-operator",
    approvedAt: new Date().toISOString()
  });
  task.status = "approved";

  addEvent("task.approved", { taskId, action });
  await persistState();
  return task;
}

async function callTool(name, args = {}) {
  switch (name) {
    case "list_agents":
      return { agents: await listWorkers() };
    case "inspect_agent": {
      if (!args.name) {
        throw new Error("name is required");
      }
      const workers = await listWorkers();
      const agent = workers.find((w) => w.name === args.name);
      if (!agent) {
        throw new Error(`Unknown worker: ${args.name}`);
      }
      return { agent };
    }
    case "submit_task": {
      if (!args.prompt || !args.repo) {
        throw new Error("prompt and repo are required");
      }
      return {
        task: await createTask({
          kind: "single",
          targetAgent: args.targetAgent,
          input: {
            prompt: args.prompt,
            repo: args.repo,
            branch: args.branch || null,
            command: args.command || null,
            testCommand: args.testCommand || null,
            baseDir: args.baseDir || null
          }
        })
      };
    }
    case "submit_task_multi": {
      if (!args.prompt || !args.repo) {
        throw new Error("prompt and repo are required");
      }
      const parsedFanout = Number.isInteger(args.fanout) && args.fanout > 0 ? args.fanout : 2;
      return {
        task: await createTask({
          kind: "multi",
          fanout: parsedFanout,
          input: {
            prompt: args.prompt,
            repo: args.repo,
            branch: args.branch || null,
            command: args.command || null,
            testCommand: args.testCommand || null,
            baseDir: args.baseDir || null
          }
        })
      };
    }
    case "task_status": {
      if (!args.taskId) {
        throw new Error("taskId is required");
      }
      const task = getTask(args.taskId);
      if (!task) {
        throw new Error(`Unknown task id: ${args.taskId}`);
      }
      return { task };
    }
    case "list_tasks":
      return { tasks: Array.from(state.tasks.values()).sort((a, b) => b.createdAt.localeCompare(a.createdAt)) };
    case "task_diff": {
      if (!args.taskId) {
        throw new Error("taskId is required");
      }
      const task = getTask(args.taskId);
      if (!task) {
        throw new Error(`Unknown task id: ${args.taskId}`);
      }
      return {
        taskId: task.id,
        status: task.status,
        review: task.review,
        diff: task.diff,
        note: "Use this output in Open WebUI for review-before-approve workflow."
      };
    }
    case "approve_action": {
      if (!args.taskId || !args.action) {
        throw new Error("taskId and action are required");
      }
      const task = await approveTask(args);
      if (!task) {
        throw new Error(`Unknown task id: ${args.taskId}`);
      }
      return { task };
    }
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

function jsonRpcError(id, code, message) {
  return {
    jsonrpc: "2.0",
    id: id || null,
    error: { code, message }
  };
}

app.get("/healthz", (_req, res) => {
  res.json({ ok: true, service: "dockweave-mcp-bridge", at: new Date().toISOString() });
});

app.get("/diagnostics", async (_req, res) => {
  const workers = await listWorkers();
  res.json({
    service: "dockweave-mcp-bridge",
    workerPrefix,
    dockerInspection,
    workers,
    inFlightTaskCount: state.inFlight.size,
    taskCount: state.tasks.size,
    recentEvents: state.events.slice(-20)
  });
});

app.get("/tools", (_req, res) => {
  res.json({ tools: TOOL_DEFINITIONS });
});

app.post("/mcp", async (req, res) => {
  const body = req.body || {};
  const id = body.id ?? null;

  try {
    if (body.jsonrpc !== "2.0" || typeof body.method !== "string") {
      return res.status(400).json(jsonRpcError(id, -32600, "Invalid Request"));
    }

    if (body.method === "initialize") {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: "2024-11-05",
          serverInfo: {
            name: "dockweave-mcp-bridge",
            version: "0.3.0"
          },
          capabilities: {
            tools: {
              listChanged: false
            }
          }
        }
      });
    }

    if (body.method === "notifications/initialized") {
      return res.status(202).end();
    }

    if (body.method === "tools/list") {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: {
          tools: TOOL_DEFINITIONS
        }
      });
    }

    if (body.method === "tools/call") {
      const name = body.params?.name;
      const args = body.params?.arguments || {};
      if (!name) {
        return res.status(400).json(jsonRpcError(id, -32602, "Tool name is required"));
      }

      const result = await callTool(name, args);
      return res.json({
        jsonrpc: "2.0",
        id,
        result: asText(result)
      });
    }

    return res.status(404).json(jsonRpcError(id, -32601, `Method not found: ${body.method}`));
  } catch (error) {
    logger.warn({ err: error }, "mcp method failed");
    return res.status(500).json(jsonRpcError(id, -32000, error.message || "Internal error"));
  }
});

app.get("/tools/list_agents", async (_req, res) => {
  res.json(await callTool("list_agents"));
});

app.get("/tools/inspect_agent/:name", async (req, res) => {
  try {
    res.json(await callTool("inspect_agent", { name: req.params.name }));
  } catch (error) {
    res.status(404).json({ error: error.message });
  }
});

app.post("/tools/submit_task", async (req, res) => {
  try {
    const result = await callTool("submit_task", req.body || {});
    res.status(202).json(result);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.post("/tools/submit_task_multi", async (req, res) => {
  try {
    const result = await callTool("submit_task_multi", req.body || {});
    res.status(202).json(result);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.get("/tools/task_status/:taskId", async (req, res) => {
  try {
    res.json(await callTool("task_status", { taskId: req.params.taskId }));
  } catch (error) {
    res.status(404).json({ error: error.message });
  }
});

app.get("/tools/list_tasks", async (_req, res) => {
  res.json(await callTool("list_tasks"));
});

app.get("/tools/task_diff/:taskId", async (req, res) => {
  try {
    res.json(await callTool("task_diff", { taskId: req.params.taskId }));
  } catch (error) {
    res.status(404).json({ error: error.message });
  }
});

app.post("/tools/approve_action", async (req, res) => {
  try {
    res.json(await callTool("approve_action", req.body || {}));
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.use((error, _req, res, _next) => {
  logger.error({ err: error }, "unhandled error");
  res.status(500).json({ error: "internal_error" });
});

await ensureStorage();
app.listen(port, () => {
  logger.info({ port }, "dockweave mcp bridge listening");
});

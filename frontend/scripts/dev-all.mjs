import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const frontendDir = process.cwd();
const backendDir = resolve(frontendDir, "..", "backend");

const pythonCandidates = process.platform === "win32"
  ? [
      resolve(backendDir, ".venv", "Scripts", "python.exe"),
      "py -3",
      "python",
    ]
  : [
      resolve(backendDir, ".venv", "bin", "python"),
      "python3",
      "python",
    ];

const python = pythonCandidates.find((path) => existsSync(path)) ?? pythonCandidates[pythonCandidates.length - 1];

const backendCommand = `"${python}" -m uvicorn app.main:app --reload --port 8000`;
const backend = spawn(backendCommand, {
  cwd: backendDir,
  stdio: "inherit",
  shell: true,
});

const frontendCommand = process.platform === "win32" ? "npm.cmd run dev" : "npm run dev";
const frontend = spawn(frontendCommand, {
  cwd: frontendDir,
  stdio: "inherit",
  shell: true,
});

const shutdown = () => {
  backend.kill("SIGINT");
  frontend.kill("SIGINT");
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

backend.on("exit", (code) => {
  if (code && code !== 0) {
    process.exit(code);
  }
});

frontend.on("exit", (code) => {
  if (code && code !== 0) {
    process.exit(code);
  }
});

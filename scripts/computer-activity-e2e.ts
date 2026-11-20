// Checks that legacy `computer-activity-v1` reports remain accepted, but do not suppress a phone
// notification. A real `TeamApiServer` on loopback serves the frozen route and a real phone
// notification service. Two phones are registered: the owner's, and one of another member as the
// witness that an event was handled. Phone sends are recorded; nothing leaves the computer.
// Run it with `bun scripts/computer-activity-e2e.ts`.
// Writes .openbot-build/computer-activity-e2e/report.json.
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentEvent } from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { TEAM_API_ROUTES } from "@openbot/contracts/team-api-routes";
import {
  TEAM_APP_VERSION_HEADER,
  TEAM_CAPABILITIES_HEADER,
  TEAM_PROTOCOL_V9,
  TEAM_PROTOCOL_VERSION_HEADER,
  teamCapabilitiesForProtocol,
} from "@openbot/contracts/team-protocol";
import {
  COMPUTER_ACTIVITY_CAPABILITY,
  COMPUTER_ACTIVITY_CODECS,
  COMPUTER_ACTIVITY_ROUTES,
} from "@openbot/contracts/team-protocol/computer-activity-v1";
import { createOpenBotLogger } from "@openbot/logging";
import { encodeLiveActivityBytes, LIVE_ACTIVITY_SECRET_BYTES } from "@openbot/team-client/live-activity-seal";
import { plugin } from "bun";
import { Effect } from "effect";
import type { AgentBrowserHost } from "../src/backend/agent/turn-lifecycle";
import { AgentService } from "../src/backend/agent-service";
import { AgentStore } from "../src/backend/agent-store";
import { runCauseEffect } from "../src/backend/effect-boundary";
import { MailboxStore } from "../src/backend/mailbox-store";
import { OpenBotDatabase } from "../src/backend/openbot-database";
import { SidebarLayoutStore } from "../src/backend/sidebar-layout-store";
import type { NotificationRelayRequest } from "../src/main/push-notifications";
import type { TeamApiBrowser } from "../src/main/team-api/dependencies";

const OUT = resolve(import.meta.dirname, "../.openbot-build/computer-activity-e2e");
const APP_VERSION = "0.0.0-e2e";
const STEP_DEADLINE_MS = 30_000;
const OWNER = { username: "owner-user", password: "owner-password-123" };
const OWNER_PHONE = "owner-phone-session";
const WITNESS = "witness-member";
const WITNESS_PHONE = "witness-phone-session";
const logger = createOpenBotLogger("computer-activity-e2e");

plugin({
  name: "computer-activity-electron-boundary",
  setup(build) {
    class Inert {}
    build.module("electron", () => ({
      loader: "object",
      exports: {
        app: { getPath: () => tmpdir(), getVersion: () => APP_VERSION, isPackaged: false, on: () => undefined },
        BrowserWindow: class extends Inert {
          static getAllWindows() {
            return [];
          }
        },
        MessageChannelMain: class extends Inert {},
        Notification: class extends Inert {
          static isSupported() {
            return false;
          }
        },
        clipboard: {},
        desktopCapturer: {},
        dialog: {},
        ipcMain: { handle: () => undefined, removeHandler: () => undefined, on: () => undefined },
        nativeImage: {},
        net: {},
        powerMonitor: { on: () => undefined },
        safeStorage: { isEncryptionAvailable: () => false },
        screen: { getAllDisplays: () => [] },
        session: {},
        shell: { openExternal: async () => undefined },
        systemPreferences: {},
        webContents: { getAllWebContents: () => [] },
      },
    }));
  },
});
const { TeamApiServer } = await import("../src/main/team-api-server");
const { TeamStore } = await import("../src/main/team-store");
const { PushNotificationService } = await import("../src/main/push-notifications");
const { decodeComputerActivityReply } = await import("../src/main/remote-host-decoding");

interface Step {
  name: string;
  passed: boolean;
  detail?: unknown;
  error?: string;
}

const report: { passed: boolean; unverified: string[]; steps: Step[] } = {
  passed: false,
  unverified: ["A request over WebRTC. The check sends over HTTP; both paths run the same route codec."],
  steps: [],
};

async function step<T>(name: string, run: () => Promise<T>, summarize: (value: T) => unknown = (value) => value) {
  process.stdout.write(`- ${name}\n`);
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    const value = await Promise.race([
      run(),
      new Promise<never>((_, reject) => {
        deadline = setTimeout(() => reject(new Error(`No result within ${STEP_DEADLINE_MS} ms.`)), STEP_DEADLINE_MS);
      }),
    ]);
    report.steps.push({ name, passed: true, detail: summarize(value) });
    return value;
  } catch (error) {
    report.steps.push({ name, passed: false, error: error instanceof Error ? error.message : String(error) });
    throw error;
  } finally {
    clearTimeout(deadline);
  }
}

const root = await mkdtemp(join(tmpdir(), "openbot-computer-activity-"));
const userData = join(root, "user-data");
const unused = () => Effect.die(new Error("Not used by this check."));
const agentBrowser: AgentBrowserHost = {
  listTabs: () => [],
  beginTakeover: unused,
  endTakeover: () => {},
  userControlTabFor: () => null,
  awaitUserControlRelease: unused,
  resolveUploadTarget: unused,
  handleDynamicTool: unused,
  onChanged: () => () => {},
  onControlChanged: () => () => {},
  onDocumentChanged: () => () => {},
  clearControls: () => {},
  endControl: () => {},
  close: () => Effect.void,
  clearAgentSession: () => Effect.void,
  shareTab: () => undefined,
  unshareThread: () => undefined,
  agentTab: () => null,
};
const teamBrowser: TeamApiBrowser = {
  listTabs: () => [],
  getControlState: () => ({ sessions: [] }),
  open: unused,
  activate: unused,
  navigate: unused,
  reload: unused,
  stop: unused,
  takeUserControl: unused,
  startTeach: unused,
  stopTeach: () => [],
  releaseUserControl: () => {},
  releaseUserControlHeldBy: () => {},
  releaseUserControlRefused: () => {},
  close: unused,
  capturePreview: unused,
  setVisible: unused,
  getDisplayState: () => ({ tabs: [], activeTabId: null }),
  loadUrl: unused,
  isPrivate: () => false,
  startView: unused,
  dispatchViewInput: unused,
  copyViewSelection: unused,
  importSignIn: unused,
};

const database = new OpenBotDatabase(userData);
const agentStore = new AgentStore(userData, join(root, "home"), database);
const mailbox = new MailboxStore(userData, agentStore.sharedRoot, agentStore.database);
const sidebar = new SidebarLayoutStore(join(userData, "sidebar-layout.json"));
const team = new TeamStore(join(userData, "openbot-team-server-v2.json"));
let agents: AgentService | undefined;
let server: InstanceType<typeof TeamApiServer> | undefined;
let phones: InstanceType<typeof PushNotificationService> | undefined;

/** The clock of the host's phone service. */
let hostNow = Date.now();
/** Phone sends, by session, in order. */
const sent: string[] = [];
const sentWaiters = new Set<() => void>();

function nextSendTo(sessionId: string): Promise<number> {
  const from = sent.length;
  return new Promise((resolveSent) => {
    const check = () => {
      const index = sent.indexOf(sessionId, from);
      if (index < 0) return;
      sentWaiters.delete(check);
      resolveSent(index);
    };
    sentWaiters.add(check);
    check();
  });
}

async function wire(
  port: number,
  input: { method: string; path: string; body?: unknown; token?: string },
): Promise<{ status: number; body: unknown }> {
  const headers = new Headers({
    Accept: "application/json",
    [TEAM_PROTOCOL_VERSION_HEADER]: String(TEAM_PROTOCOL_V9),
    [TEAM_APP_VERSION_HEADER]: APP_VERSION,
    [TEAM_CAPABILITIES_HEADER]: teamCapabilitiesForProtocol(TEAM_PROTOCOL_V9).join(","),
  });
  if (input.body !== undefined) headers.set("Content-Type", "application/json");
  if (input.token) headers.set("Authorization", `Bearer ${input.token}`);
  const response = await fetch(`http://127.0.0.1:${port}${input.path}`, {
    method: input.method,
    headers,
    body: input.body === undefined ? null : JSON.stringify(input.body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

function registerPhone(sessionId: string, memberId: string): void {
  assert.ok(phones);
  phones.register(
    sessionId,
    { memberId, protocol: TEAM_PROTOCOL_V9, capabilities: new Set() },
    {
      platform: "android",
      value: {
        serverId: "fixture-server",
        token: `${sessionId}-token-`.padEnd(40, "x"),
        secret: encodeLiveActivityBytes(randomBytes(LIVE_ACTIVITY_SECRET_BYTES)),
        locale: "en",
        away: true,
        level: "all",
        mutedUntil: null,
        preview: false,
        sound: "default",
      },
    },
  );
}

function failedTurn(agentId: string, threadId: string, turnId: string): AgentEvent {
  return { type: "turn-completed", agentId, threadId, turnId, status: "failed" };
}

try {
  const setup = await step("Start the host with phone notifications, and sign in the desktop", async () => {
    await runCauseEffect(sidebar.initialize());
    await runCauseEffect(agentStore.initialize());
    await runCauseEffect(mailbox.initialize());
    await runCauseEffect(team.initialize());
    const agent = await runCauseEffect(
      agentStore.createAgent({
        name: "Activity fixture",
        description: "Synthetic agent for the computer activity check",
        avatarSeed: "computer-activity",
        avatarHue: null,
      }),
    );
    agents = new AgentService({ store: agentStore, mailbox, browser: agentBrowser, sidebarLayout: sidebar });
    await runCauseEffect(team.configure("E2E Host", OWNER.username, OWNER.password));
    const ownerId = team.getOwnerMemberId();
    assert.ok(ownerId, "The host has no owner.");
    const service = agents;
    phones = new PushNotificationService({
      agents: service,
      store: { list: () => [], replace: () => Effect.void },
      send: (request: NotificationRelayRequest) =>
        Effect.sync(() => {
          sent.push(request.push.sessionId);
          for (const wake of [...sentWaiters]) wake();
          return "sent" as const;
        }),
      hiddenAgentIds: () => new Set(),
      memberActive: () => true,
      liveActivityShows: () => false,
      randomBytes: (size) => new Uint8Array(randomBytes(size)),
      now: () => hostNow,
      logger,
    });
    server = new TeamApiServer({
      store: team,
      agents: service,
      mailbox,
      browser: teamBrowser,
      sidebarLayout: sidebar,
      appVersion: APP_VERSION,
      pushNotifications: phones,
    });
    const port = await Effect.runPromise(server.start());
    phones.start();
    registerPhone(OWNER_PHONE, ownerId);
    registerPhone(WITNESS_PHONE, WITNESS);
    const login = await wire(port, { method: "POST", path: "/v1/auth/login", body: OWNER });
    assert.equal(login.status, 200, `Login failed: ${JSON.stringify(login.body)}`);
    assert.ok(isDynamicRecord(login.body) && typeof login.body.sessionToken === "string");
    return { port, token: login.body.sessionToken, agentId: agent.id, threadId: agent.threadId ?? "activity-thread" };
  });
  const { port, token, agentId, threadId } = setup;
  const event = (turnId: string) => failedTurn(agentId, threadId, turnId);
  assert.ok(agents && phones && server);
  const service = agents;

  async function phonesFor(turnId: string): Promise<{ owner: boolean }> {
    const from = sent.length;
    const witness = nextSendTo(WITNESS_PHONE);
    service.emit("event", event(turnId));
    const index = await witness;
    return { owner: sent.slice(from, index).includes(OWNER_PHONE) };
  }

  /** Sends a legacy desktop report through the frozen Team API codec. */
  async function reportActivity(idleSeconds: number): Promise<boolean> {
    const body = { idleSeconds };
    const codec = COMPUTER_ACTIVITY_CODECS.get(COMPUTER_ACTIVITY_ROUTES.report);
    assert.ok(codec, "The route has no codec.");
    codec.request(body);
    const response = await wire(port, { method: "POST", path: COMPUTER_ACTIVITY_ROUTES.report, body, token });
    assert.equal(response.status, 200, `Activity failed: ${JSON.stringify(response.body)}`);
    return decodeComputerActivityReply(codec.response(response.status, response.body));
  }

  await step("The host advertises computer-activity-v1 to a protocol 9 peer", async () => {
    const compatibility = await wire(port, { method: "GET", path: TEAM_API_ROUTES.compatibility });
    assert.ok(isDynamicRecord(compatibility.body) && Array.isArray(compatibility.body.capabilities));
    assert.ok(compatibility.body.capabilities.includes(COMPUTER_ACTIVITY_CAPABILITY), "The capability is missing.");
    return { advertised: true };
  });

  await step("Without input, the owner's phone gets the event", async () => {
    const result = await phonesFor("turn-before-input");
    assert.equal(result.owner, true, "The owner's phone got no notification.");
    return result;
  });

  await step("A legacy input report does not suppress the owner's phone notification", async () => {
    const phonesKnown = await reportActivity(0);
    assert.equal(phonesKnown, true, "The host did not report the registered phone.");
    const result = await phonesFor("turn-during-input");
    assert.equal(result.owner, true, "The owner's phone was suppressed while the desktop had input.");
    return { phonesKnown, ...result };
  });

  await step("A legacy report stays passive and does not count as hosted-server use", async () => {
    await reportActivity(0);
    assert.equal(server?.lastClientUseAt() ?? null, null, "An activity report moved the client use time.");
    return { lastClientUseAt: null };
  });

  await step("A report that says the computer is idle also leaves phone delivery unchanged", async () => {
    hostNow += 2_000;
    const phonesKnown = await reportActivity(2);
    assert.equal(phonesKnown, true, "The host did not report the registered phone.");
    const result = await phonesFor("turn-after-away");
    assert.equal(result.owner, true, "The owner's phone was suppressed after the desktop became idle.");
    return { phonesKnown, ...result };
  });

  await step("A legacy report says when the member has no registered phone", async () => {
    phones?.remove(OWNER_PHONE);
    const phonesKnown = await reportActivity(0);
    assert.equal(phonesKnown, false, "The host reported a phone after its registration was removed.");
    return { phonesKnown };
  });

  report.passed = true;
} finally {
  if (phones) await Effect.runPromise(phones.dispose()).catch(() => undefined);
  if (server) await Effect.runPromise(server.stop()).catch(() => undefined);
  if (agents) await runCauseEffect(agents.stop()).catch(() => undefined);
  database.close();
  await rm(root, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`Report: ${join(OUT, "report.json")}\n`);
}

if (!report.passed) process.exitCode = 1;

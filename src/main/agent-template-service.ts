import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  agentTemplateShareOrigin,
  createAgentTemplateShareUrl,
  isAgentTemplateId,
  OPENBOT_AGENT_TEMPLATE_ORIGIN,
} from "@openbot/contracts/agent-template-links";
import { AVATAR_IMAGE_LIMITS, INPUT_LIMITS } from "@openbot/contracts/input-limits";
import {
  AGENT_TEMPLATE_LIMITS,
  type AgentSummary,
  type AgentTemplateDetail,
  type AgentTemplatePreview,
  type AgentTemplatePublication,
  type AgentTemplateSkill,
  type AgentTemplateSnapshot,
  type AgentTemplateSnapshotProblem,
  type AvatarImageInput,
  agentTemplateSnapshotProblem,
  decodeAgentTemplateDetail,
  type InstallAgentTemplateInput,
  type InstallAgentTemplateResult,
  isAgentTemplateCardPng,
  isAgentTemplateSnapshot,
  isReleasedRoutineSchedule,
  type PublishAgentTemplateInput,
  type RoutineSchedule,
  toAgentTemplateSnapshot,
} from "@openbot/contracts/ipc";
import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import { sourceText } from "@openbot/i18n/source";
import { containsSecret } from "@openbot/logging";
import { Effect, Result, Schema } from "effect";
import type { AgentLifecycleFailed } from "../backend/agent-service";
import { causeHelpers } from "../backend/effect-boundary";
import { imageMimeType, localSchedule, toArrayBuffer, validTimezone } from "./agent-marketplace-service";
import type { CentralAuthOperationError } from "./central-auth-effects";
import type { LocalSkillLibrary } from "./local-skill-library";
import { MANAGED_SKILL_SLUGS } from "./managed-skill-service";
import type { SkillMarketplaceFailure } from "./skill-marketplace-service";
import { inspectSkillMarkdown, normalizedFiles } from "./skill-package";

function snapshotProblemText(problem: AgentTemplateSnapshotProblem): string {
  switch (problem.kind) {
    case "name":
      return sourceText("error.marketplace.templateName", { count: INPUT_LIMITS.agentName });
    case "title":
      return sourceText("error.marketplace.templateRole", { count: INPUT_LIMITS.agentTitle });
    case "noInstructions":
      return sourceText("error.marketplace.templateNoInstructions");
    case "instructions":
      return sourceText("error.marketplace.templateInstructions", { count: INPUT_LIMITS.agentDescription });
    case "avatar":
      return sourceText("error.marketplace.templateAvatar");
    case "skills":
      return sourceText("error.marketplace.templateSkills", { count: AGENT_TEMPLATE_LIMITS.skills });
    case "localSkills":
      return sourceText("error.marketplace.templateLocalSkills", { count: AGENT_TEMPLATE_LIMITS.embeddedSkills });
    case "skill":
      return sourceText("error.marketplace.templateSkill", { name: problem.name });
    case "routines":
      return sourceText("error.marketplace.templateRoutines", { count: INPUT_LIMITS.agentRoutines });
    case "routine":
      return sourceText("error.marketplace.templateRoutine", {
        name: problem.name || sourceText("error.marketplace.templateRoutineNoName"),
        count: INPUT_LIMITS.routineName,
      });
    case "tooLarge":
      return sourceText("error.marketplace.templateTooLarge");
  }
}

const SKILL_STAGING = ".openbot/template-skills";

interface AgentTemplateAuth {
  requestAuthorized<T>(
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    timeoutMs?: number,
  ): Effect.Effect<T, CentralAuthOperationError>;
  downloadAuthorized(path: string): Effect.Effect<Uint8Array, CentralAuthOperationError>;
  resolveApiUrl(path: string): string;
}

interface AgentTemplateAgents {
  listAgents(): AgentSummary[];
  listRoutines(agentId: string): Array<{
    name: string;
    instruction: string;
    active: boolean;
    trigger: { schedule: RoutineSchedule };
  }>;
  resolveAvatar(agentId: string): { path: string; mimeType: AvatarImageInput["mimeType"] } | null;
  createAgentProfile(input: {
    name: string;
    title?: string;
    description: string;
    avatarSeed: string;
    avatarHue: AgentTemplateSnapshot["avatarHue"];
  }): Effect.Effect<AgentSummary, AgentLifecycleFailed>;
  setAvatar(agentId: string, image: AvatarImageInput | null): Effect.Effect<AgentSummary, AgentLifecycleFailed>;
  createRoutine(
    input: {
      agentId: string;
      name: string;
      instruction: string;
      active: boolean;
      timezone: string;
      schedule: RoutineSchedule;
    },
    options?: { recordConversationEvent?: boolean },
  ): { id: string };
  deleteAgent(agentId: string): Effect.Effect<void, AgentLifecycleFailed>;
}

interface AgentTemplateSkills {
  listTemplateSkills(agentId: string): Effect.Effect<AgentTemplateSkill[], SkillMarketplaceFailure>;
  installVersion(input: {
    agentId: string;
    skillId: string;
    versionId: string;
  }): Effect.Effect<unknown, SkillMarketplaceFailure>;
  installPublicVersion(input: {
    agentId: string;
    skillId: string;
    versionId: string;
  }): Effect.Effect<unknown, SkillMarketplaceFailure>;
  library(): Pick<LocalSkillLibrary, "list" | "create" | "revise" | "bundle" | "withdraw">;
  installLocal(input: {
    agentId: string;
    skillId: string;
    revision: number;
  }): Effect.Effect<unknown, SkillMarketplaceFailure>;
}

interface OwnedTemplate {
  id: string;
  sourceAgentId: string;
  updatedAt: string;
}

interface LoadedAgentTemplate {
  detail: AgentTemplateDetail;
  source: "account" | "public";
}

type PublicFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export class AgentTemplateService {
  constructor(
    private readonly auth: AgentTemplateAuth,
    private readonly agents: AgentTemplateAgents,
    private readonly skills: AgentTemplateSkills,
    private readonly publicFetch: PublicFetch = fetch,
  ) {}

  preview(agentId: string): Effect.Effect<AgentTemplatePreview, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<AgentTemplatePreview, AgentTemplateFailure> {
      const agent = yield* templateSync(() => this.requireAgent(agentId));
      const [owned, skills, avatarImage] = yield* Effect.all(
        [
          this.owned(agentId).pipe(Effect.catch(() => Effect.succeed(null))),
          this.skills
            .listTemplateSkills(agentId)
            .pipe(toAgentTemplateFailure)
            .pipe(
              Effect.map((value) => ({ value, error: null })),
              Effect.catch((error) => Effect.succeed({ value: [], error: message(error.cause) })),
            ),
          this.readAvatar(agentId).pipe(Effect.catch(() => Effect.succeed(null))),
        ],
        { concurrency: "unbounded" },
      );
      const skillsError = skills.error;
      return {
        ...this.profile(agent, skills.value),
        agentId,
        avatarUrl: agent.avatarUrl,
        avatarImage,
        updatedAt: agent.updatedAt,
        publication: owned ? this.publication(owned) : null,
        skillsError,
      };
    });
  }

  publish({ agentId, card }: PublishAgentTemplateInput): Effect.Effect<AgentTemplatePublication, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<AgentTemplatePublication, AgentTemplateFailure> {
      if (card && !isAgentTemplateCardPng(card))
        return yield* new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.shareCardInvalid")) });
      const agent = yield* templateSync(() => this.requireAgent(agentId));
      const snapshot = yield* this.snapshot(agent);
      const problem = agentTemplateSnapshotProblem(snapshot);
      if (problem) return yield* new AgentTemplateFailure({ cause: new Error(snapshotProblemText(problem)) });
      if (!isAgentTemplateSnapshot(snapshot))
        return yield* new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.cannotPublish")) });
      yield* templateSync(() => assertNoSecrets(snapshot));
      const form = new FormData();
      form.set("snapshot", JSON.stringify(snapshot));
      form.set("sourceAgentId", agentId);
      const avatar = yield* this.readAvatar(agentId);
      if (avatar) form.set("avatar", new Blob([toArrayBuffer(avatar.bytes)], { type: avatar.mimeType }), "avatar");
      if (card) form.set("card", new Blob([toArrayBuffer(card)], { type: "image/png" }), "card.png");
      const owned = yield* this.auth
        .requestAuthorized("/v1/agent-templates/", { method: "POST", body: form }, decodeOwnedTemplate, 30_000)
        .pipe(toAgentTemplateFailure);
      return this.publication(owned);
    });
  }

  unpublish(agentId: string): Effect.Effect<void, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<void, AgentTemplateFailure> {
      const owned = yield* this.owned(agentId);
      if (!owned) return;
      yield* this.auth
        .requestAuthorized(`/v1/agent-templates/${encodeURIComponent(owned.id)}`, { method: "DELETE" }, decodeDeleted)
        .pipe(toAgentTemplateFailure);
    });
  }

  get(templateId: string): Effect.Effect<AgentTemplateDetail, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<AgentTemplateDetail, AgentTemplateFailure> {
      const loaded = yield* this.loadTemplate(templateId);
      return yield* templateSync(() => this.resolveTemplateUrls(loaded));
    });
  }

  install(input: InstallAgentTemplateInput): Effect.Effect<InstallAgentTemplateResult, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<InstallAgentTemplateResult, AgentTemplateFailure> {
      if (!validTimezone(input.timezone))
        return yield* new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.timezoneInvalid")) });
      const loaded = yield* this.loadTemplate(input.templateId);
      const detail = yield* templateSync(() => this.resolveTemplateUrls(loaded));
      if (detail.updatedAt !== input.expectedUpdatedAt)
        return yield* new AgentTemplateFailure({
          cause: new Error(sourceText("error.marketplace.changedSinceOpened")),
        });
      const library = yield* templateSync(() => this.skills.library());
      const localSkills = yield* library.list().pipe(toAgentTemplateFailure);
      const existingSkills = new Map<string, { id: string; revision: number; sameText: boolean }>();
      for (const skill of detail.skills) {
        if (skill.kind !== "embedded") continue;
        const { slug } = yield* templateSync(() => inspectSkillMarkdown(skill.markdown));
        const current = localSkills.find((candidate) => candidate.slug === slug);
        if (!current) continue;
        const text = new TextDecoder().decode(
          normalizedFiles(yield* library.bundle(current.id, current.version).pipe(toAgentTemplateFailure))["SKILL.md"],
        );
        existingSkills.set(slug, { id: current.id, revision: current.version, sameText: text === skill.markdown });
      }
      let avatar: AvatarImageInput | null = null;
      if (detail.avatarUrl) {
        const avatarUrl = detail.avatarUrl;
        const bytes =
          loaded.source === "public"
            ? yield* this.downloadPublicAvatar(avatarUrl)
            : yield* this.auth.downloadAuthorized(avatarUrl).pipe(toAgentTemplateFailure);
        const mimeType = imageMimeType(bytes);
        if (!mimeType)
          return yield* new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.avatarInvalid")) });
        avatar = { mimeType, bytes };
      }

      let agent = yield* this.agents
        .createAgentProfile({
          name: detail.name,
          ...(detail.title ? { title: detail.title } : {}),
          description: detail.description,
          avatarSeed: detail.avatarSeed,
          avatarHue: detail.avatarHue,
        })
        .pipe(toAgentTemplateFailure);
      const published: Array<{ id: string; revision: number }> = [];
      {
        const outcome = yield* Effect.result(
          Effect.gen({ self: this }, function* () {
            for (const skill of detail.skills) {
              if (skill.kind === "marketplace") {
                const install = { agentId: agent.id, skillId: skill.skillId, versionId: skill.versionId };
                if (loaded.source === "public")
                  yield* this.skills.installPublicVersion(install).pipe(toAgentTemplateFailure);
                else yield* this.skills.installVersion(install).pipe(toAgentTemplateFailure);
                continue;
              }
              const existing = existingSkills.get(inspectSkillMarkdown(skill.markdown).slug);
              if (existing?.sameText) {
                yield* this.skills
                  .installLocal({ agentId: agent.id, skillId: existing.id, revision: existing.revision })
                  .pipe(toAgentTemplateFailure);
                continue;
              }
              const folder = `${SKILL_STAGING}/${skill.slug}`;
              const target = join(agent.workspacePath, ...folder.split("/"));
              yield* Effect.gen({ self: this }, function* () {
                yield* templateIO(() => mkdir(target, { recursive: true }));
                yield* templateIO(() => writeFile(join(target, "SKILL.md"), skill.markdown, { flag: "wx" }));
                const revision = yield* Effect.gen(function* () {
                  const revision = yield* (
                    existing
                      ? library.revise(agent.id, existing.id, existing.revision, folder)
                      : library.create(agent.id, folder)
                  ).pipe(toAgentTemplateFailure);
                  published.push({ id: revision.id, revision: revision.version });
                  return revision;
                }).pipe(Effect.uninterruptible);
                yield* this.skills
                  .installLocal({ agentId: agent.id, skillId: revision.id, revision: revision.version })
                  .pipe(toAgentTemplateFailure);
              }).pipe(
                Effect.ensuring(
                  Effect.gen({ self: this }, function* () {
                    yield* templateIO(() => rm(target, { recursive: true, force: true }));
                  }).pipe(Effect.orDie),
                ),
              );
            }
            for (const routine of detail.routines)
              yield* templateSync(() =>
                this.agents.createRoutine(
                  {
                    agentId: agent.id,
                    name: routine.name,
                    instruction: routine.instruction,
                    active: routine.active,
                    timezone: input.timezone,
                    schedule: localSchedule(routine.schedule),
                  },
                  { recordConversationEvent: false },
                ),
              );
            if (avatar) agent = yield* this.agents.setAvatar(agent.id, avatar).pipe(toAgentTemplateFailure);
          }),
        );
        if (Result.isFailure(outcome)) {
          const error = outcome.failure.cause;
          yield* this.agents.deleteAgent(agent.id).pipe(Effect.catch(() => Effect.void));
          for (const skill of published.reverse())
            yield* library.withdraw(skill.id, skill.revision).pipe(Effect.catch(() => Effect.void));
          return yield* new AgentTemplateFailure({ cause: error });
        }
      }
      return { agent };
    });
  }

  snapshot(agent: AgentSummary): Effect.Effect<AgentTemplateSnapshot, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<AgentTemplateSnapshot, AgentTemplateFailure> {
      return this.profile(agent, yield* this.skills.listTemplateSkills(agent.id).pipe(toAgentTemplateFailure));
    });
  }

  private loadTemplate(templateId: string): Effect.Effect<LoadedAgentTemplate, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<LoadedAgentTemplate, AgentTemplateFailure> {
      if (!isAgentTemplateId(templateId))
        return yield* new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.linkInvalid")) });
      const local = yield* Effect.result(
        this.auth
          .requestAuthorized(
            `/v1/agent-templates/${encodeURIComponent(templateId)}`,
            { method: "GET" },
            decodeAgentTemplateDetail,
          )
          .pipe(toAgentTemplateFailure),
      );
      if (Result.isSuccess(local)) return { detail: local.success, source: "account" };
      // Public links belong to OpenBot. A self-hosted account service cannot read them.
      return { detail: yield* this.readPublicTemplate(templateId), source: "public" };
    });
  }

  private resolveTemplateUrls({ detail, source }: LoadedAgentTemplate): AgentTemplateDetail {
    const skills = detail.skills.filter((skill) => skill.kind !== "embedded" || !MANAGED_SKILL_SLUGS.has(skill.slug));
    if (!detail.avatarUrl) return { ...detail, skills, avatarUrl: null };
    const avatarUrl =
      source === "account"
        ? accountAvatarUrl(this.auth, detail.avatarUrl)
        : publicAvatarUrl(detail.avatarUrl, detail.id);
    return { ...detail, skills, avatarUrl };
  }

  private readPublicTemplate(templateId: string): Effect.Effect<AgentTemplateDetail, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* () {
      const url = new URL(`/v1/agent-templates/${encodeURIComponent(templateId)}`, OPENBOT_AGENT_TEMPLATE_ORIGIN);
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          this.publicFetch(url, {
            headers: { accept: "application/json" },
            credentials: "omit",
            cache: "no-store",
            redirect: "error",
            signal: AbortSignal.any([signal, AbortSignal.timeout(30_000)]),
          }),
        catch: () => unreadableTemplate(),
      });
      if (!response.ok) return yield* unreadableTemplate();
      const value = yield* Effect.tryPromise({ try: () => response.json(), catch: () => unreadableTemplate() });
      const detail = yield* Effect.try({
        try: () => decodeAgentTemplateDetail(value),
        catch: () => unreadableTemplate(),
      });
      if (detail.id !== templateId) return yield* unreadableTemplate();
      return detail;
    });
  }

  private downloadPublicAvatar(urlValue: string): Effect.Effect<Uint8Array, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* () {
      let url: URL;
      try {
        url = new URL(urlValue);
      } catch {
        return yield* unreadableTemplate();
      }
      if (url.origin !== OPENBOT_AGENT_TEMPLATE_ORIGIN) return yield* unreadableTemplate();
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          this.publicFetch(url, {
            credentials: "omit",
            cache: "no-store",
            redirect: "error",
            signal: AbortSignal.any([signal, AbortSignal.timeout(30_000)]),
          }),
        catch: () => unreadableTemplate(),
      });
      if (!response.ok) return yield* unreadableTemplate();
      const contentLength = Number(response.headers.get("Content-Length"));
      if (Number.isFinite(contentLength) && contentLength > AVATAR_IMAGE_LIMITS.sourceBytes)
        return yield* unreadableTemplate();
      return yield* Effect.tryPromise({
        try: () => readLimitedResponse(response, AVATAR_IMAGE_LIMITS.sourceBytes),
        catch: () => unreadableTemplate(),
      });
    });
  }

  private profile(agent: AgentSummary, skills: AgentTemplateSkill[]): AgentTemplateSnapshot {
    return toAgentTemplateSnapshot({
      name: agent.name,
      title: agent.title,
      description: agent.description,
      avatarSeed: agent.avatarSeed,
      avatarHue: agent.avatarHue,
      skills,
      routines: this.agents
        .listRoutines(agent.id)
        .flatMap(({ name, instruction, active, trigger }) =>
          isReleasedRoutineSchedule(trigger.schedule)
            ? [{ name, instruction, active, schedule: trigger.schedule }]
            : [],
        ),
    });
  }

  readAvatar(agentId: string): Effect.Effect<AvatarImageInput | null, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<AvatarImageInput | null, AgentTemplateFailure> {
      const avatar = this.agents.resolveAvatar(agentId);
      if (!avatar) return null;
      return { mimeType: avatar.mimeType, bytes: new Uint8Array(yield* templateIO(() => readFile(avatar.path))) };
    });
  }

  owned(agentId: string): Effect.Effect<OwnedTemplate | null, AgentTemplateFailure> {
    return Effect.gen({ self: this }, function* (): Effect.fn.Return<OwnedTemplate | null, AgentTemplateFailure> {
      const mine = yield* this.auth
        .requestAuthorized("/v1/agent-templates/mine", { method: "GET" }, decodeOwnedTemplates)
        .pipe(toAgentTemplateFailure);
      return mine.find((template) => template.sourceAgentId === agentId) ?? null;
    });
  }

  private publication(owned: OwnedTemplate): AgentTemplatePublication {
    const origin = agentTemplateShareOrigin(new URL(this.auth.resolveApiUrl("/")).origin);
    return {
      templateId: owned.id,
      shareUrl: createAgentTemplateShareUrl(owned.id, origin),
      publishedAt: owned.updatedAt,
    };
  }

  private requireAgent(agentId: string): AgentSummary {
    const agent = this.agents.listAgents().find((candidate) => candidate.id === agentId);
    if (!agent) throw new Error(sourceText("error.skill.chooseLocalAgent"));
    return agent;
  }
}

function assertNoSecrets(snapshot: AgentTemplateSnapshot): void {
  const fields: Array<[string, string]> = [
    [sourceText("error.marketplace.secretInName"), snapshot.name],
    [sourceText("error.marketplace.secretInTitle"), snapshot.title],
    [sourceText("error.marketplace.secretInInstructions"), snapshot.description],
    ...snapshot.routines.flatMap((routine): Array<[string, string]> => {
      const message = sourceText("error.marketplace.secretInRoutine", { name: routine.name });
      return [
        [message, routine.name],
        [message, routine.instruction],
      ];
    }),
    ...snapshot.skills.flatMap(
      (skill): Array<[string, string]> =>
        skill.kind === "embedded"
          ? [[sourceText("error.marketplace.secretInSkill", { name: skill.name }), skill.markdown]]
          : [],
    ),
  ];
  for (const [message, text] of fields) if (containsSecret(text)) throw new Error(message);
}

function isOwnedTemplate(value: unknown): value is OwnedTemplate {
  return (
    isDynamicRecord(value) &&
    isString(value.id) &&
    isAgentTemplateId(value.id) &&
    isString(value.sourceAgentId) &&
    isString(value.updatedAt)
  );
}

function decodeOwnedTemplate(value: unknown): OwnedTemplate {
  if (!isOwnedTemplate(value)) throw new Error("Invalid agent template response.");
  return { id: value.id, sourceAgentId: value.sourceAgentId, updatedAt: value.updatedAt };
}

function decodeOwnedTemplates(value: unknown): OwnedTemplate[] {
  if (!Array.isArray(value)) throw new Error("Invalid agent template list.");
  return value.map(decodeOwnedTemplate);
}

function decodeDeleted(value: unknown): { deleted: true } {
  if (!isDynamicRecord(value) || value.deleted !== true) throw new Error("Invalid agent template response.");
  return { deleted: true };
}

function message(error: unknown): string {
  return error instanceof Error && error.message ? error.message : "The skills could not be read.";
}

export class AgentTemplateFailure extends Schema.TaggedError<AgentTemplateFailure>()("AgentTemplateFailure", {
  cause: Schema.Defect(),
}) {}

const { io: templateIO, sync: templateSync, rewrap: toAgentTemplateFailure } = causeHelpers(AgentTemplateFailure);

function publicAvatarUrl(value: string, templateId: string): string {
  let url: URL;
  try {
    url = new URL(value, OPENBOT_AGENT_TEMPLATE_ORIGIN);
  } catch {
    throw new Error(sourceText("error.marketplace.templateUnreadable"));
  }
  if (
    url.origin !== OPENBOT_AGENT_TEMPLATE_ORIGIN ||
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== `/v1/agent-templates/${encodeURIComponent(templateId)}/avatar` ||
    url.hash !== "" ||
    (url.search !== "" && (url.searchParams.size !== 1 || !/^\d+$/u.test(url.searchParams.get("v") ?? "")))
  )
    throw new Error(sourceText("error.marketplace.templateUnreadable"));
  return url.toString();
}

function accountAvatarUrl(auth: AgentTemplateAuth, value: string): string {
  const api = new URL(auth.resolveApiUrl("/"));
  const url = new URL(auth.resolveApiUrl(value));
  if (url.origin !== api.origin || url.username !== "" || url.password !== "" || url.protocol !== api.protocol)
    throw new Error(sourceText("error.marketplace.templateUnreadable"));
  return url.toString();
}

function unreadableTemplate(): AgentTemplateFailure {
  return new AgentTemplateFailure({ cause: new Error(sourceText("error.marketplace.templateUnreadable")) });
}

async function readLimitedResponse(response: Response, limit: number): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (!reader) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > limit) throw new Error("Response is too large.");
    return bytes;
  }
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) {
        await reader.cancel();
        throw new Error("Response is too large.");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

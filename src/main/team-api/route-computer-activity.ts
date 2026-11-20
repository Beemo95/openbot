import {
  COMPUTER_ACTIVITY_CAPABILITY,
  COMPUTER_ACTIVITY_ROUTES,
} from "@openbot/contracts/team-protocol/computer-activity-v1";
import type { PushNotificationService } from "../push-notifications";
import type { RouteOutcome, TeamApiRequestContext } from "./request-context";
import { readJson } from "./request-helpers";

/**
 * Accepts legacy computer-activity reports. Phone notifications no longer depend on desktop
 * activity. The response still tells older clients whether this host has a phone registration.
 */
export async function routeComputerActivity(
  context: TeamApiRequestContext,
  notifications: PushNotificationService | undefined,
): Promise<RouteOutcome> {
  const { method, url, capabilities, request, json, member } = context;
  if (method !== "POST" || url.pathname !== COMPUTER_ACTIVITY_ROUTES.report) return "unmatched";
  if (!notifications || !capabilities.has(COMPUTER_ACTIVITY_CAPABILITY)) return "unmatched";
  const { idleSeconds } = await readJson(request);
  if (typeof idleSeconds !== "number") return "unmatched";
  return json(200, { phones: notifications.hasRegisteredPhone(member.id) });
}

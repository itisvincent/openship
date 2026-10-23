/**
 * The autoscaler decision loop.
 *
 * One tick per project policy: fresh observation → eligibility/drift gates →
 * hysteresis → clamp → ±step → apply IN PLACE under the project's runtime lock.
 * The apply path is the whole feature: a scale step must never become a
 * blue/green release (double capacity while old and new both run), so this
 * patches `spec.replicas` on the ACTIVE release through the same verified
 * transport deploys use, then commits the new intent to the project config.
 *
 * Guardrails (from the design review):
 *  - the loop judges OBSERVED replicas (the runtime status), never saved intent;
 *  - every 409 means "hold and re-read", never a retry storm;
 *  - a stopped project (disabledAt) is a human state the loop stays out of;
 *  - drift between observed replicas and saved config without one of this
 *    loop's scale events means someone else moved the count: suspend;
 *  - missing metrics fail closed (hold), never read as zero load;
 *  - failures back off exponentially and suspend after N consecutive throws.
 */

import { reportCaughtError as observeCaughtError } from "@repo/core/diagnostics";
import { repos, type Project } from "@repo/db";
import {
  AppError,
  clusterWorkloadNeedsOperator,
  hasCpuLimit,
  safeErrorMessage,
  type AutoscaleEvent,
} from "@repo/core";
import { KubernetesApiError } from "@repo/adapters";
import { env } from "../../../config/env";
import { findActiveDeployment } from "../../../lib/active-deployment";
import { withDeploymentRuntime } from "../../../lib/deployment-runtime";
import { withLiveProjectRuntimeMutation } from "../../../lib/project-runtime-lock";
import type { DeploymentMeta } from "../../../lib/deployment-runtime";

/** Hysteresis band around the CPU target: a load delta inside ±H does not move
 * the count. Without it, the loop oscillates around the target forever. */
const HYSTERESIS_PERCENT = 10;

/** Scale-downs need a longer settle than scale-ups; usage spikes recover in
 * seconds but a genuine drop needs a full traffic pattern to prove itself. */
const SCALE_DOWN_MIN_COOLDOWN_SECONDS = 300;

const eventRow = (
  action: AutoscaleEvent["action"],
  fromReplicas: number,
  toReplicas: number | null,
  observedCpuPercent: number | null,
  reason: string,
): Omit<AutoscaleEvent, "id"> => ({
  createdAt: new Date().toISOString(),
  action,
  fromReplicas,
  toReplicas,
  observedCpuPercent,
  reason,
});

export function autoscaleEligibilityReason(
  p: Project,
  meta: DeploymentMeta | null,
): string | null {
  if (env.CLOUD_MODE) return "Autoscaling is available on self-hosted OpenShip.";
  if (p.appTemplateId === "openship")
    return "The OpenShip control plane cannot be autoscaled.";
  if (!p.clusterId) return "This project does not run on a server cluster.";
  if (!meta?.clusterId || meta.clusterId !== p.clusterId)
    return "Deploy this project to its selected cluster first.";
  const production = p.resources as
    | { production?: { cpuCores?: number } | null }
    | null;
  if (!hasCpuLimit(production?.production))
    return "Set a CPU limit for this project first: per-instance load is measured against its limit.";
  if (
    clusterWorkloadNeedsOperator(p.framework) ||
    clusterWorkloadNeedsOperator(p.appTemplateId)
  )
    return "Databases are replicated through database management, not instance scaling.";
  return null;
}

/**
 * Apply one replica change in place, under the project's runtime lock; return
 * the committed count. Conflicts throw `CLUSTER_WORKLOAD_CONFLICT` and the
 * tick treats them as "hold and re-read next tick".
 */
async function applyScale(
  projectId: string,
  observedDeploymentId: string,
  desired: number,
): Promise<void> {
  await withLiveProjectRuntimeMutation(projectId, async (liveProject: Project) => {
    // Re-read everything the decision was made from: the release the loop
    // observed must still BE the active release, and no build/deploy may be
    // in flight. Both checks inside the runtime lock close the window a
    // concurrent deploy leaves between observation and apply.
    const { checkNoActiveBuild } = await import("../../deployments/build.service");
    await checkNoActiveBuild(projectId);
    const current = await findActiveDeployment(liveProject);
    if (!current || current.id !== observedDeploymentId || !current.containerId)
      throw new AppError(
        "The active release changed while scaling it. No replica change was applied.",
        409,
        "CLUSTER_WORKLOAD_CONFLICT",
      );
    const meta = current.meta as DeploymentMeta | null;
    if (meta?.clusterId !== liveProject.clusterId)
      throw new AppError(
        "The project's cluster target changed while scaling it. No replica change was applied.",
        409,
        "CLUSTER_WORKLOAD_CONFLICT",
      );
    const saved = liveProject.clusterConfig ?? meta?.clusterConfig ?? null;
    if (!saved)
      throw new AppError(
        "This project's cluster configuration is unavailable, so the replica intent cannot be saved.",
        409,
        "CLUSTER_CONFIG_REQUIRED",
      );
    await withDeploymentRuntime(current, async (rt) => {
      if (!rt.supports("scaleInPlace") || !rt.scaleInPlace)
        throw new AppError(
          "The active release's runtime cannot change its instance count in place.",
          409,
          "CLUSTER_WORKLOAD_UNSUPPORTED",
        );
      await rt.scaleInPlace(current.containerId!, desired);
    });
    // Commit the new intent with the same single-writer write every scale
    // uses, so dashboards, preconditions and the next release all agree.
    await repos.clusterRuntime.bindProject(
      liveProject.organizationId,
      projectId,
      liveProject.clusterId,
      { ...saved, replicas: desired },
      liveProject.updatedAt.toISOString(),
    );
    await repos.deployment.patchMetaReplicas(current.id, desired);
  });
}

export interface AutoscaleTickResult {
  projectId: string;
  applied?: AutoscaleEvent["action"];
  held?: string;
  failed?: string;
}

export async function runAutoscaleTick(): Promise<{
  evaluated: number;
  results: AutoscaleTickResult[];
}> {
  if (env.CLOUD_MODE) return { evaluated: 0, results: [] };
  // Global kill switch first — it stops every policy in one row write.
  const instance = await repos.instanceSettings.get();
  if (instance?.autoscalerDisabled) return { evaluated: 0, results: [] };
  const policies = await repos.autoscale.listEnabled();
  const results: AutoscaleTickResult[] = [];
  for (const policy of policies) {
    // Backoff gate: failures push the next attempt into the future.
    if (policy.nextAttemptAt && policy.nextAttemptAt.getTime() > Date.now()) {
      results.push({ projectId: policy.projectId, held: "retrying after backoff" });
      continue;
    }
    try {
      const outcome = await scaleOnePolicy(policy);
      await repos.autoscale.recordSuccess(policy.projectId);
      if (outcome) results.push(outcome);
    } catch (error) {
      // A conflict is a hold, not a failure: someone else holds the lock, the
      // workload CAS, or a Kubernetes optimistic-concurrency precondition.
      // Re-read next tick; never burn backoff or suspend on these.
      if (
        error instanceof AppError &&
        (["CLUSTER_WORKLOAD_CONFLICT", "CLUSTER_RUNTIME_CONFLICT"].includes(error.code ?? "") ||
          (error instanceof KubernetesApiError && error.statusCode === 409))
      ) {
        results.push({ projectId: policy.projectId, held: "another operation holds the project" });
        continue;
      }
      observeCaughtError(error, "platform/engine/modules/projects/autoscaler");
      await repos.autoscale
        .recordFailure(policy.projectId, policy.suspendAfterFailures)
        .catch((diagnosticFailure) =>
          observeCaughtError(
            diagnosticFailure,
            "platform/engine/modules/projects/autoscaler",
          ),
        );
      const reason = safeErrorMessage(error);
      const row = {
        ...eventRow("fail", 0, null, null, reason),
        id: `fail-${Date.now()}`,
      };
      await repos.autoscale
        .setLastDecision(policy.projectId, row)
        .catch(() => undefined);
      results.push({ projectId: policy.projectId, failed: reason });
    }
  }
  return { evaluated: policies.length, results };
}

// K8s-HPA replica formula, then the hysteresis band, the ±step and the
// min/max clamps. Only a load delta outside ±10% of the target moves the
// count; that is what keeps a loop-free steady state steady.
export function decideReplicas(
  observed: number,
  utilization: number,
  policy: {
    targetCpuPercent: number;
    minReplicas: number;
    maxReplicas: number;
    stepSize: number;
  },
): { direction: "scale_up" | "scale_down"; desired: number } | null {
  const formula = Math.ceil((observed * utilization) / policy.targetCpuPercent);
  const band = (policy.targetCpuPercent * HYSTERESIS_PERCENT) / 100;
  const inBand =
    utilization <= policy.targetCpuPercent + band &&
    utilization >= policy.targetCpuPercent - band;
  const direction =
    !inBand && formula > observed
      ? "scale_up"
      : !inBand && formula < observed
        ? ("scale_down" as const)
        : null;
  if (!direction) return null;
  let desired =
    direction === "scale_up"
      ? Math.min(observed + policy.stepSize, policy.maxReplicas)
      : Math.max(observed - policy.stepSize, policy.minReplicas);
  desired = Math.min(desired, policy.maxReplicas);
  desired = Math.max(desired, policy.minReplicas);
  if (desired === observed) return null;
  return { direction, desired };
}

/** One project's decision. Returns a TickResult only when something is worth
 * surfacing (an applied change or a meaningful hold); quiet no-ops return null. */
async function scaleOnePolicy(
  policy: Awaited<ReturnType<typeof repos.autoscale.listEnabled>>[number],
): Promise<AutoscaleTickResult | null> {
  const projectId = policy.projectId;
  if (policy.suspendedAt) return { projectId, held: "policy is suspended" };
  const project = await repos.project.findById(projectId);
  if (!project || project.deletionInProgress) return null;
  if (project.disabledAt) return { projectId, held: "the project is stopped" };
  const active = await findActiveDeployment(project);
  if (!active?.containerId || active.containerId === "compose")
    return { projectId, held: "no scalable active release" };
  const meta = active.meta as DeploymentMeta | null;

  const eligibility = autoscaleEligibilityReason(project, meta);
  if (eligibility) return { projectId, held: eligibility };

  // Fresh observation — status and metrics read now, never cached.
  const observation = await withDeploymentRuntime(active, async (rt) => {
    if (!rt.status || !rt.supports("podMetrics") || !rt.podMetrics) return null;
    const [status, metrics] = await Promise.all([
      rt.status(active.containerId!),
      rt.podMetrics(active.containerId!),
    ]);
    return { status, metrics };
  });
  if (!observation)
    return { projectId, held: "observing the active release failed" };

  const { status, metrics } = observation;
  const observed = status.desired;
  if (!observed) return { projectId, held: "the release is not running" };
  // An in-progress rollout is not a decision moment: wait for it to finish,
  // so the cooldown measures rollout completion, not intent time.
  if (status.observedGeneration < status.generation)
    return { projectId, held: "the current rollout is still in progress" };

  // Drift gate. `config.replicas` is the saved intent; the autoscaler keeps it
  // in lockstep with observed replicas. Divergence here means something else
  // moved the count without touching the policy: suspend, never clamp back —
  // an incident operator outranks this loop every time.
  const savedReplicas = meta?.clusterConfig?.replicas;
  if (savedReplicas !== undefined && savedReplicas !== observed) {
    const reason = `Instances changed outside the autoscaler (${savedReplicas} → ${observed}). It suspended this policy and is no longer changing the instance count.`;
    await repos.autoscale.suspend(projectId, reason);
    const row = { ...eventRow("suspend", savedReplicas, null, null, reason), id: `suspend-${Date.now()}` };
    await repos.autoscale.insertEvent(projectId, row);
    await repos.autoscale.setLastDecision(projectId, row);
    return { projectId, held: reason };
  }

  // Per-instance CPU utilization against the project's CPU limit. The K8s
  // release sets requests = limits, so this is the same denominator HPA uses.
  const cpuLimitMillicores =
    ((project.resources as { production?: { cpuCores?: number } | null } | null)
      ?.production?.cpuCores ?? 0) * 1000;
  const pods = metrics.pods.filter((pod) => pod.cpuMillicores !== null);
  if (!pods.length)
    return { projectId, held: "instance CPU usage is unavailable" };
  const avgCpuMillicores =
    pods.reduce((sum, pod) => sum + (pod.cpuMillicores ?? 0), 0) / pods.length;
  const utilization = (avgCpuMillicores / cpuLimitMillicores) * 100;

  // K8s-HPA replica formula, then the hysteresis band, the ±step and the
  // min/max clamps. Only a load delta outside ±10% of the target moves the
  // count; that is what keeps a loop-free steady state steady.
  const decision = decideReplicas(observed, utilization, policy);
  if (!decision) return { projectId, held: "no change needed" };
  const { direction, desired } = decision;

  // Cooldown, measured from the last COMPLETED step, never from intent time.
  const lastScale = await repos.autoscale.lastScaleEvent(projectId);
  if (lastScale?.createdAt) {
    const elapsedMs = Date.now() - lastScale.createdAt.getTime();
    if (elapsedMs >= 0) {
      // Scale-downs need a longer settle than scale-ups; usage spikes recover
      // in seconds but a genuine drop needs a full traffic pattern to prove it.
      const cooldownSeconds =
        lastScale.action === "scale_down"
          ? Math.max(lastScale.cooldownSeconds ?? 0, SCALE_DOWN_MIN_COOLDOWN_SECONDS)
          : (lastScale.cooldownSeconds ?? policy.cooldownSeconds);
      if (elapsedMs < cooldownSeconds * 1000)
        return { projectId, held: "cooling down after the last change" };
    }
  }

  const row = {
    ...eventRow(
      direction,
      observed,
      desired,
      utilization,
      `${Math.round(utilization)}% of the CPU limit vs a ${policy.targetCpuPercent}% target`,
    ),
    id: `dec-${Date.now()}`,
  };
  await applyScale(projectId, active.id, desired);
  await repos.autoscale.insertEvent(projectId, row);
  await repos.autoscale.setLastDecision(projectId, row);
  return { projectId, applied: direction };
}
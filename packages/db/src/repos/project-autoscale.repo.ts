import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { AppError, generateId } from "@repo/core";
import type { AutoscaleEvent } from "@repo/core";
import type { Database } from "../client";
import {
  project,
  projectAutoscalePolicy,
  projectAutoscaleEvent,
} from "../schema";
import { withProjectConfigurationWrite } from "./project-work-admission";

/** Recent events per project are capped so one hot project cannot grow the
 * history unbounded; the autoscaler inserts at most one row per tick. */
const EVENTS_PER_PROJECT = 100;

export type AutoscalePolicyRecord = typeof projectAutoscalePolicy.$inferSelect;
export type AutoscaleEventRecord = typeof projectAutoscaleEvent.$inferSelect;

export interface SetAutoscalePolicyInput {
  enabled: boolean;
  targetCpuPercent: number;
  minReplicas: number;
  maxReplicas: number;
  stepSize: number;
  cooldownSeconds: number;
  suspendAfterFailures: number;
  /** Operator confirms a suspended policy should resume ownership. */
  rearm?: true;
  /** CAS precondition against the saved policy row; ignored when absent. */
  expectedUpdatedAt?: string;
}

const FAILURE_BACKOFF_MS = (failures: number) =>
  Math.min(60_000 * 2 ** Math.max(0, failures - 1), 30 * 60_000);

export function createAutoscaleRepo(db: Database) {
  const owned = (projectId: string) => eq(projectAutoscalePolicy.projectId, projectId);

  return {
    async get(projectId: string): Promise<AutoscalePolicyRecord | null> {
      const [row] = await db.select().from(projectAutoscalePolicy).where(owned(projectId));
      return row ?? null;
    },

    async listEnabled(): Promise<AutoscalePolicyRecord[]> {
      return db
        .select()
        .from(projectAutoscalePolicy)
        .where(eq(projectAutoscalePolicy.enabled, 1));
    },

    /** Only this path (operator, via the dashboard) may clear a suspension.
     *  CAS on the saved policy's updatedAt keeps two dashboard tabs from
     *  silently overwriting each other (or an operator's re-arm from landing
     *  on a policy the loop just re-suspended). */
    async save(
      projectId: string,
      input: SetAutoscalePolicyInput,
    ): Promise<AutoscalePolicyRecord> {
      return withProjectConfigurationWrite(db, eq(project.id, projectId), async (tx) => {
        const now = new Date();
        const [existing] = await tx
          .select()
          .from(projectAutoscalePolicy)
          .where(owned(projectId))
          .for("update");
        if (
          input.expectedUpdatedAt &&
          existing &&
          existing.updatedAt.toISOString() !== input.expectedUpdatedAt
        )
          throw new AppError(
            "The autoscaling policy changed. Reload before applying these settings.",
            409,
            "AUTOSCALE_POLICY_CONFLICT",
          );
        const rearming = input.rearm === true && input.enabled;
        const values = {
          projectId,
          enabled: input.enabled ? 1 : 0,
          targetCpuPercent: input.targetCpuPercent,
          minReplicas: input.minReplicas,
          maxReplicas: input.maxReplicas,
          stepSize: input.stepSize,
          cooldownSeconds: input.cooldownSeconds,
          suspendAfterFailures: input.suspendAfterFailures,
          suspendedAt: rearming ? null : (existing?.suspendedAt ?? null),
          suspendedReason: rearming ? null : (existing?.suspendedReason ?? null),
          nextAttemptAt: rearming ? null : (existing?.nextAttemptAt ?? null),
          failureCount: rearming ? 0 : (existing?.failureCount ?? 0),
          lastDecision: existing?.lastDecision ?? null,
          createdAt: existing?.createdAt ?? now,
          updatedAt: now,
        };
        const [row] = await tx
          .insert(projectAutoscalePolicy)
          .values(values)
          .onConflictDoUpdate({
            target: projectAutoscalePolicy.projectId,
            set: values,
          })
          .returning();
        return row;
      });
    },

    /** Disable by operator: stops the loop and clears pending backoff. */
    async disable(projectId: string): Promise<void> {
      await db
        .update(projectAutoscalePolicy)
        .set({
          enabled: 0,
          suspendedAt: null,
          suspendedReason: null,
          nextAttemptAt: null,
          failureCount: 0,
          updatedAt: new Date(),
        })
        .where(owned(projectId));
    },

    /** The autoscaler, or an override detector, suspends ownership with a
     * reason; only `save(..., { rearm: true })` clears it afterwards. An
     * already-suspended row keeps its FIRST and most human-relevant reason. */
    async suspend(projectId: string, reason: string): Promise<void> {
      await db
        .update(projectAutoscalePolicy)
        .set({ suspendedAt: new Date(), suspendedReason: reason, updatedAt: new Date() })
        .where(and(owned(projectId), isNull(projectAutoscalePolicy.suspendedAt)));
    },

    async rearm(projectId: string): Promise<void> {
      await withProjectConfigurationWrite(db, eq(project.id, projectId), async (tx) => {
        await tx
          .update(projectAutoscalePolicy)
          .set({
            suspendedAt: null,
            suspendedReason: null,
            nextAttemptAt: null,
            failureCount: 0,
            updatedAt: new Date(),
          })
          .where(owned(projectId));
      });
    },

    async recordFailure(
      projectId: string,
      suspendAfterFailures: number,
    ): Promise<void> {
      const [row] = await db
        .update(projectAutoscalePolicy)
        .set({
          failureCount: sql`${projectAutoscalePolicy.failureCount} + 1`,
          updatedAt: new Date(),
        })
        .where(owned(projectId))
        .returning();
      if (!row) return;
      if (row.failureCount >= suspendAfterFailures) {
        await this.suspend(
          projectId,
          `The autoscaler failed ${row.failureCount} times in a row and gave up. Review the cluster, then re-arm it.`,
        );
        return;
      }
      await db
        .update(projectAutoscalePolicy)
        .set({ nextAttemptAt: new Date(Date.now() + FAILURE_BACKOFF_MS(row.failureCount)) })
        .where(owned(projectId));
    },

    async recordSuccess(projectId: string): Promise<void> {
      await db
        .update(projectAutoscalePolicy)
        .set({ failureCount: 0, nextAttemptAt: null, updatedAt: new Date() })
        .where(owned(projectId));
    },

    async setLastDecision(projectId: string, decision: AutoscaleEvent): Promise<void> {
      await db
        .update(projectAutoscalePolicy)
        .set({ lastDecision: decision, updatedAt: new Date() })
        .where(owned(projectId));
    },

    async insertEvent(
      projectId: string,
      event: Omit<AutoscaleEvent, "id">,
      opts?: { cooldownSeconds?: number },
    ): Promise<void> {
      await db.insert(projectAutoscaleEvent).values({
        id: generateId("ase"),
        projectId,
        action: event.action,
        fromReplicas: event.fromReplicas,
        toReplicas: event.toReplicas,
        observedCpuPercent:
          event.observedCpuPercent === null ? null : Math.round(event.observedCpuPercent),
        cooldownSeconds: opts?.cooldownSeconds ?? null,
        reason: event.reason,
      });
      // Prune to the newest cap in a single pass.
      await db.execute(sql`
        DELETE FROM "project_autoscale_event"
        WHERE "project_id" = ${projectId}
        AND "id" NOT IN (
          SELECT "id" FROM "project_autoscale_event"
          WHERE "project_id" = ${projectId}
          ORDER BY "created_at" DESC, "id" DESC
          LIMIT ${EVENTS_PER_PROJECT}
        )`);
    },

    /** The most recent applied step (up or down), for cooldown timing. Holds
     *  and failures never reset the window. */
    async lastScaleEvent(projectId: string): Promise<AutoscaleEventRecord | null> {
      const [row] = await db
        .select()
        .from(projectAutoscaleEvent)
        .where(
          and(
            eq(projectAutoscaleEvent.projectId, projectId),
            sql`${projectAutoscaleEvent.action} IN ('scale_up','scale_down')`,
          ),
        )
        .orderBy(desc(projectAutoscaleEvent.createdAt), desc(projectAutoscaleEvent.id))
        .limit(1);
      return row ?? null;
    },

    async listEvents(projectId: string): Promise<AutoscaleEventRecord[]> {
      return db
        .select()
        .from(projectAutoscaleEvent)
        .where(eq(projectAutoscaleEvent.projectId, projectId))
        .orderBy(desc(projectAutoscaleEvent.createdAt), desc(projectAutoscaleEvent.id))
        .limit(EVENTS_PER_PROJECT);
    },
  };
}

export type AutoscaleRepo = ReturnType<typeof createAutoscaleRepo>;
import { beforeAll, beforeEach, afterAll, describe, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { migrate } from "drizzle-orm/pglite/migrator";
import { fileURLToPath } from "node:url";
import * as schema from "../schema";
import { createAutoscaleRepo } from "./project-autoscale.repo";

const client = new PGlite("memory://");
const db = drizzle(client, { schema });
const autoscale = createAutoscaleRepo(db);
let orgId = "org";
let projectId = "proj";

const POLICY = {
  enabled: true,
  targetCpuPercent: 70,
  minReplicas: 1,
  maxReplicas: 6,
  stepSize: 1,
  cooldownSeconds: 120,
  suspendAfterFailures: 3,
};

const seed = async () => {
  await db.insert(schema.organization).values({ id: orgId, name: "Org" });
  await db.insert(schema.projectGroup).values({
    id: "group",
    name: "App",
    slug: "app",
    organizationId: orgId,
  });
  await db.insert(schema.project).values({
    id: projectId,
    organizationId: orgId,
    groupId: "group",
    name: "App",
    slug: "app",
  });
};

beforeAll(async () => {
  await migrate(db, { migrationsFolder: fileURLToPath(new URL("../../drizzle", import.meta.url)) });
}, 30_000);
afterAll(async () => {
  await client.close();
});
beforeEach(async () => {
  await db.delete(schema.projectAutoscaleEvent);
  await db.delete(schema.projectAutoscalePolicy);
  await db.delete(schema.project);
  await db.delete(schema.organization);
  await seed();
});

describe("repos.autoscale", () => {
  it("saves and re-reads one policy per project", async () => {
    const saved = await autoscale.save(projectId, POLICY);
    expect(saved.enabled).toBe(1);
    expect(saved.targetCpuPercent).toBe(70);
    const again = await autoscale.save(projectId, { ...POLICY, targetCpuPercent: 60 });
    expect(again.id).toBe(saved.id);
    expect(again.targetCpuPercent).toBe(60);
    const list = await autoscale.listEnabled();
    expect(list).toHaveLength(1);
  });

  it("suspension is sticky: only rearm clears it", async () => {
    await autoscale.save(projectId, POLICY);
    await autoscale.suspend(projectId, "a human touched replicas");
    const suspended = await autoscale.get(projectId);
    expect(suspended?.suspendedReason).toBe("a human touched replicas");
    // A second suspend keeps the FIRST and most human-relevant reason.
    await autoscale.suspend(projectId, "something else");
    expect((await autoscale.get(projectId))?.suspendedReason).toBe("a human touched replicas");
    await autoscale.rearm(projectId);
    const rearmed = await autoscale.get(projectId);
    expect(rearmed?.suspendedAt).toBeNull();
    expect(rearmed?.suspendedReason).toBeNull();
  });

  it("saving with rearm clears suspension, saving without keeps it", async () => {
    await autoscale.save(projectId, POLICY);
    await autoscale.suspend(projectId, "manual override");
    await autoscale.save(projectId, { ...POLICY, targetCpuPercent: 80 });
    expect((await autoscale.get(projectId))?.suspendedAt).not.toBeNull();
    const result = await autoscale.save(projectId, { ...POLICY, rearm: true });
    expect(result.suspendedAt).toBeNull();
    expect(result.targetCpuPercent).toBe(70);
  });

  it("disable clears pending backoff and suspension intent", async () => {
    await autoscale.save(projectId, POLICY);
    await autoscale.suspend(projectId, "reason");
    await autoscale.disable(projectId);
    const row = await autoscale.get(projectId);
    expect(row?.enabled).toBe(0);
    expect(row?.suspendedAt).toBeNull();
    expect(row?.nextAttemptAt).toBeNull();
    expect(await autoscale.listEnabled()).toHaveLength(0);
  });

  it("backoff grows exponentially and a failure streak suspends", async () => {
    await autoscale.save(projectId, POLICY);
    const before = Date.now();
    await autoscale.recordFailure(projectId, 3);
    let row = await autoscale.get(projectId);
    expect(row?.failureCount).toBe(1);
    const gap1 = row!.nextAttemptAt!.getTime() - before;
    expect(gap1).toBeGreaterThan(0);
    await autoscale.recordFailure(projectId, 3);
    row = await autoscale.get(projectId);
    const gap2 = (row?.nextAttemptAt?.getTime() ?? 0) - before;
    expect(gap2).toBeGreaterThan(gap1); // exponential
    await autoscale.recordFailure(projectId, 3);
    row = await autoscale.get(projectId);
    expect(row?.suspendedAt).not.toBeNull();
    expect(row?.suspendedReason).toContain("3 times in a row");
    // The tick's recordSuccess unblocks again on success — but only via rearm
    // once suspended, matching the dashboard handshake.
    await autoscale.rearm(projectId);
    await autoscale.recordSuccess(projectId);
    row = await autoscale.get(projectId);
    expect(row?.failureCount).toBe(0);
    expect(row?.nextAttemptAt).toBeNull();
  });

  it("keeps a bounded event trail and finds the last applied step", async () => {
    await autoscale.save(projectId, POLICY);
    for (let idx = 0; idx < 12; idx++) {
      await autoscale.insertEvent(projectId, {
        createdAt: new Date(Date.now() + idx).toISOString(),
        action: "hold",
        fromReplicas: 2,
        toReplicas: null,
        observedCpuPercent: 50,
        reason: `hold-${idx}`,
      });
    }
    await autoscale.insertEvent(
      projectId,
      {
        createdAt: new Date().toISOString(),
        action: "scale_up",
        fromReplicas: 2,
        toReplicas: 3,
        observedCpuPercent: 90,
        reason: "spike",
      },
      { cooldownSeconds: 120 },
    );
    const events = await autoscale.listEvents(projectId);
    expect(events).toHaveLength(13);
    expect(events[0].action).toBe("scale_up");
    const lastScale = await autoscale.lastScaleEvent(projectId);
    expect(lastScale?.action).toBe("scale_up");
    expect(lastScale?.cooldownSeconds).toBe(120);
    expect(lastScale?.toReplicas).toBe(3);
  });
});
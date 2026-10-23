# Project autoscaling (self-hosted clusters)

OpenShip ships an in-app autoscaler for projects that run on a Kubernetes
(K3s) server cluster. It is self-hosted only — the same scope as the cluster
feature itself — and it is deliberately **in-app**: only OpenShip can make a
scale step cheap, show its state in the dashboard, and stop cleanly when a
human takes over.

## What a scale step is

A scale step is **not** a deployment. The autoscaler patches `spec.replicas`
on the active release in place (the same mechanism Start/Stop use), atomically
with the saved intent annotation and the project's cluster configuration.
Kubernetes creates or removes pods from the same immutable image; services
never go blue/green, and no double capacity is ever needed.

A full configuration release only happens when something other than the count
changes — exactly like manual scaling.

## How the loop works

A recurring system job (`projects:autoscaler`, every 30 seconds) evaluates each
enabled policy:

1. **Observe** the active release: replica status and per-instance CPU usage
   from the cluster's bundled metrics source, read over OpenShip's verified
   mTLS transport. Nothing is installed; the metrics source K3s ships is used
   as-is. Missing metrics fail closed — the tick holds, it never reads as zero
   load.
2. **Gate**: the release must be running and its rollout complete; a stopped
   project (or one mid-deploy) is a human state the loop stays out of.
3. **Decide**: average per-instance CPU against the project's CPU limit,
   compared to the policy's target with a ±10% hysteresis band, then the
   ±step clamp and the min/max limits. Scale-downs wait longer than scale-ups
   (a drop must prove itself across the cooldown window).
4. **Apply** the change in place under the project's runtime lock, record the
   decision and its reason in the per-project event trail.

The cooldown is measured from the last **completed** step, not from intent
time, so a slow rollout cannot shorten the next window.

## The override handshake

A human outranks the autoscaler, every time:

- Any manual replica change (manual scale, cluster-target change) **suspends**
  the policy automatically. The autoscaler stops owning the count and the
  dashboard shows why, with a re-arm button.
- The dashboard's save + **re-arm** is the only way the loop resumes.
- Failure backoff: repeated tick failures retry with exponential backoff and
  give up (suspend) after the configured number of consecutive failures.

## Kill switches

- **Per project**: disable the policy (or leave it suspended).
- **Global**: instance setting `autoscaler_disabled` stops every tick.

## Limits you should know about

- **Control-plane SPOF.** The first control server is both the autoscaler's
  brain and the only path to the K8s API. When it dies, the autoscaler goes
  blind and inert — existing replicas keep running and keep serving; no new
  scaling decisions happen until it returns. This does not change ingress
  availability.
- **Ingress HA is separate.** OpenShip Edge sends traffic through one cluster
  server today. Autoscaling does not fix that; put an external load balancer /
  floating IP in front if you need it.
- **CPU limits are required.** Per-instance load is measured against the
  project's CPU limit; a project without limits cannot set a policy.
- **Not supported**: database projects (replicate through database
  management), static sites, multi-service environments on Kubernetes, and
  the OpenShip control plane itself.

## Metrics

The dashboard's scaling panel gains per-instance CPU/memory once the release
runs on Kubernetes (the bundled metrics source). The cluster read contract
returns it as `metrics` alongside pod readiness.
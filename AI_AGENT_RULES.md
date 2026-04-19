# AI agent rules — Glassbox STS Tool (`gb_sts_tool`)

These rules are **mandatory** for any automated or assistant-driven change in this repository.

## Product intent

- Local-only web UI bound to loopback by default (`GB_STS_BIND`, `GB_STS_PORT`).
- Talks to **production Kubernetes** via the operator’s workstation `kubectl`; the app must stay **trustworthy**: read-first, explicit confirmations for anything that can mutate state.
- **Versioning**: `server.py` defines `VERSION` (semver). On every software update, bump patch (`1.0.1`, `1.0.2`, …) or minor/major as appropriate, and mirror the same string in:
  - `index.html` cache-bust query (`css/app.css?v=`, `js/app.js?v=`)
  - `js/app.js` `VERSION` constant (footer label)

## Safety and trust (non-negotiable)

1. **Default read-only**: Prefer fixed, audited panel commands in `server.py` / `backend/k8s_exec.py`. Do not add silent write paths.
2. **Destructive / write operations** (topic delete, alter, consumer group delete, offset reset with execute, partition reassignment execute, arbitrary `javac`/`java` admin hacks, etc.) must:
   - Stay **off** auto-refresh paths.
   - Require a **two-step** operator flow: UI confirmation + server-side `confirmed` flag (see `/api/kafka/exec`).
3. **Never** weaken `classify_command_risk` without an explicit human security review in the same PR.
4. **Sanitize** all user-supplied names passed into shell: only `[A-Za-z0-9_.-]+` (enforced by `sanitize_name`).
5. **Always** prefix in-pod Kafka tooling with `export JMX_PORT=3333;` (see `kafka_bash_exec`).
6. **Session gate**: Kafka APIs require a signed cookie from `/api/k8s/verify`. Do not bypass session checks.

## Kubernetes UX

- Always show **current kubectl context** in the UI header (`/api/k8s/context`).
- Cluster “connect” verifies: substring match on context or API server URL, `kubectl cluster-info`, pod `glassbox-kafka-0` reachable (namespace auto-discovered unless overridden).
- **EKS / AWS**: Named profiles are a **fixed preset list** in `backend/aws_profiles.py` (`GET /api/aws/profiles`). The app does **not** read `~/.aws/credentials`. The chosen profile is stored in the signed session (`p`) and passed as **`AWS_PROFILE`** to every `kubectl` subprocess when `session.d` (cloud) is `aws`. For **Azure** sessions, `AWS_PROFILE` is cleared for kubectl. Do not log profile secrets. Last connection choices are written to **`data/sensitive/last_connected.json`** (gitignored except `.gitkeep`). To add or rename profiles, edit `AWS_PROFILE_OPTIONS` / `AWS_PROFILE_DEFAULT` in that module and bump the app version.
- **Kafka exec**: Always `kubectl exec … -c kafka …`; use a **random high `JMX_PORT` per request** inside the pod (not a fixed 3333) to avoid JMX bind collisions.

## Code style

- Match existing patterns: stdlib `http.server`, small `backend/` modules, vanilla JS (no bundler unless explicitly requested).
- Keep diffs minimal and scoped; no unrelated refactors.

## When adding features

- Read `FUTURE_INSTRUCTIONS.md` for the roadmap and constraints.
- If you add HTTP routes, update `agent_instructions.json` `http_api.routes`.
- New pods (Elasticsearch, OpenSearch, etc.) should follow the same **verify → gated nav → read-only panels → confirm writes** pattern as Kafka.

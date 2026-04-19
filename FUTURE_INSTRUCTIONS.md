# Future work and release discipline — `gb_sts_tool`

Instructions for humans and coding agents working in this folder.

## Version bumps

On **every** software update:

1. Increment `VERSION` in `server.py` (e.g. `1.0.1`, `1.0.2`, … for small fixes).
2. Update cache-bust query strings in `index.html` for `css/app.css?v=` and `js/app.js?v=`.
3. Update the `VERSION` constant in `js/app.js` (footer).
4. Update `agent_instructions.json` → `repo.app_version` if present.

Agents must follow **`AI_AGENT_RULES.md`** and **`agent_instructions.json`**.

## Planned data stores (reference only)

Stateful pods on the target cluster (for future pages; **v1.0.x implements Kafka only**):

| System        | Pod name                          |
|---------------|-----------------------------------|
| Kafka         | `glassbox-kafka-0`                |
| Elasticsearch | `glassbox-elasticsearch-master-0` |
| OpenSearch    | `glassbox-opensearch-master-0`    |
| Clingine      | `clingine-0`                      |
| ClickHouse    | `glassbox-clickhouse-0`           |
| PostgreSQL    | `glassbox-postgresql-0`           |
| Cassandra     | `glassbox-cassandra-0`            |

Each future service page should reuse: **context strip**, **session verify**, **read-only panels with per-panel refresh**, **two-step confirm** for any write.

## Kafka roadmap ideas

- Optional topic picker populated from `consumer_groups_list` / topics panel.
- Lag script style summary (threshold filter) as a dedicated panel with **read-only** output.
- Reassignment / delete / offset reset: **do not** implement without design sign-off; if added, must stay behind confirmations and never auto-run.

## Ops

- For long-lived sessions across server restarts, set `GB_STS_COOKIE_SECRET` in the environment; otherwise the signing key is ephemeral per process.
- Last Home connection selections are stored in `data/sensitive/last_connected.json` (mode `0600`, directory `0700`). The directory is gitignored except `data/sensitive/.gitkeep`.

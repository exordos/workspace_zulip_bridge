# Workspace Provider Entity API dependency

The owning contract lives in the `workspace_backend` repository at
`docs/en/provider_entity_api.md`. Its generated OpenAPI document is the
machine-readable source of truth.

This bridge implements bootstrap schema version `2` over the Workspace
`/v1/provider/` endpoints. In particular it depends on:

- `GET /v1/provider/bootstrap?mode=paged`;
- paged `GET /v1/provider/entities/{type}` reads;
- atomic `POST /v1/provider/entities/actions/apply/invoke` writes;
- stable UUID identity and SHA-256 `content_hash` values;
- UTC ISO-8601 `source_updated_at` ordering;
- the manifest `epoch_generation` and `snapshot_epoch_version` continuation
  cursor.

Do not extend these payloads in this repository. Contract changes start in
`workspace_backend`; the bridge is updated only after the owning contract has
been reviewed.

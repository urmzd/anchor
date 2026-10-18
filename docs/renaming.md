# Ecosystem naming migration

| Previous | Current | Responsibility |
|----------|---------|----------------|
| Collegium and Vultus | Warp | Shared human-and-agent workspace, API and presentation |
| Saige | Saige | Agent execution and evaluations |
| Mandatum | Fiber | Event bus, connectors and request routing |
| Legatus | Playscape | Governed execution environments and workspace isolation |
| Duraturo | Anchor | Durable execution, replay and recovery |

This is a breaking source and configuration rename. Update Go imports to `github.com/urmzd/anchor`, `github.com/urmzd/playscape`, `github.com/urmzd/fiber` and `github.com/urmzd/warp`. Saige's durable backend moves from `agent/durable/duraturo` to `agent/durable/anchor`. Warp UI packages use `@warp/*` and live with the platform in one pnpm workspace.

Update CLI names, environment prefixes (`ANCHOR_`, `PLAYSCAPE_`, `FIBER_`, `WARP_`), service names and deployment references together. Existing deployments should back up state and explicitly move any old dot-directories to their new names before starting the renamed binaries. Warp's cookies and OAuth scopes now use `warp`; update identity-provider configuration and sign in again.

Persisted Fiber database table names, replay activity and record names, agent-spec hash domains, protobuf package and service identities, and Saige wire error codes retain their original values. They are compatibility contracts, not branding. Historical changelog entries and legacy replay fixtures keep their original names too.

## Release order

1. Anchor, including its Postgres and Redis adapter modules, and Playscape.
2. Saige with its Anchor backend and pinned Anchor dependencies.
3. Fiber with pinned Anchor, Playscape and Saige dependencies.
4. Warp with the platform and presentation together and pinned library dependencies.

The old Vultus repository remains available as history. Active UI development belongs in Warp.

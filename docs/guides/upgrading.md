# Names

Humagen is the platform formerly named Warp. Library names are unchanged.
Stored identifiers may retain earlier names for compatibility.

| Name | Responsibility |
|------|----------------|
| [Humagen](https://github.com/urmzd/humagen) | The platform: the one process, its API, console and Slack app |
| Fiber | The library for the event bus, connectors, routing and the agent runtime |
| Saige | The library for agent execution and evaluations |
| Playscape | The library for governed execution environments and workspace isolation |
| Anchor | The library for durable execution, replay and recovery |

Earlier names (Warp, Collegium and Vultus for Humagen, Mandatum for Fiber, Legatus for
Playscape, Duraturo for Anchor) are retired. See the [Humagen upgrade guide](https://github.com/urmzd/humagen/blob/main/docs/guides/upgrading-from-warp.md)
for accepted old names and identifiers kept for stored data.

The earlier library rename changed these identifiers:

| Identifier | Was | Is | Upgrade |
|------------|-----|----|---------|
| Postgres tables and indexes | `mandatum_*` | `fiber_*` | renamed in place at startup |
| Durable activity and record names | `mandatum.*` | `fiber.*` | drain runs in flight before upgrading; they are not replayed across it |
| Saige wire error codes | `durable.duraturo.*` | `durable.anchor.*` | upgrade every process together |
| GitHub review format | `mandatum.github.review.v1` | `warp.github.review.v1` | agents' prompts name the new one (see `examples/github-review`) |
| GitHub `/fix` grant | `mandatum.github.mutation.v1` | `warp.github.mutation.v1` | a grant issued before the upgrade is refused; ask for a new `/fix` |

New GitHub review instructions use `humagen.github.review.v1`;
`warp.github.review.v1` is still accepted. The `/fix` grant domain stays
`warp.github.mutation.v1`, so grants issued before the Humagen rename still verify.

Environment variables use `HUMAGEN_`, `FIBER_`, `PLAYSCAPE_` and `ANCHOR_`.

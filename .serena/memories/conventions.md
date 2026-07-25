# Project conventions
- DDD placement: entity/state-machine behavior in `domain/entity`; strategy decisions in `domain/service` or `domain/strategy`; DB/file/SQL/network details in `infrastructure`; cross-domain orchestration in `application`.
- Keep API JSON field names backward-compatible unless breaking change is requested.
- SQL generation must be deterministic, schema-qualified where established, and table-driven tested.
- Preserve user worktree changes; never fold unrelated dirty files into commits.
- Config changes require TOML examples, env overrides, validation, docs, and deployment templates where applicable.
- Storage changes must cover file and MySQL paths plus AES-GCM password encryption compatibility; never persist by mutating the live plaintext task.
- Lifecycle/concurrency changes require joint review of `TaskService`, runtime maps, cancellation, scheduler, and shutdown cleanup.
- Destructive target behavior such as DROP TABLE must remain explicit.
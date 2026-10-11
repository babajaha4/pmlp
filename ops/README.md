VPS service recovery
====================

Install `polymaker-live.service` into `/etc/systemd/system/` after stopping the
old transient unit and backing up SQLite. Run `systemctl daemon-reload`.
Installation does not authorize a start or enable automatic startup at boot.
Run `/home/ubuntu/.local/bin/uv sync --locked` on every deployment to restore
the project virtual environment, including the development/test group.

Authoritative startup read failures exit with status 78 and remain stopped.
Inspect the log and reconcile before an explicit start. Other failures retry
after 60 seconds, with at most five starts per hour. The local control panel
supports the installed service as well as the legacy transient service.

Retained on-chain proofs are not manually cleared. Recovery requires an
initialized ledger, no pending trades, fresh chain balances matching REST,
ledger explanation for all configured positions, a successful open-orders
snapshot, and unchanged fill/proof/placement revisions before publishing.
Kill flags and same-day risk baselines are preserved. A new UTC baseline is
established only after authoritative startup reconciliation.

`journald-pmlp.conf` installs in `/etc/systemd/journald.conf.d/` and bounds
system journal retention to 100 MiB. `rsyslog-rotation.conf` replaces the
stock weekly rsyslog rotation after backing up the original; invoke its
normal `/etc/logrotate.conf` state at least hourly. Install
`logrotate-hourly.conf` as `/etc/systemd/system/logrotate.timer.d/pmlp.conf`,
reload systemd, and restart `logrotate.timer` to schedule that check hourly.
Archived log pruning is
irreversible. Preserve incident evidence before applying retention.
The existing five-minute PolyMaker logrotate timer separately manages
application logs and market journal (50 MiB threshold, two compressed
archives each). Check their combined actual size stays below 500 MiB after
every deployment; these files do not include SQLite or database backups.

Dashboard daily PnL is unavailable when its baseline belongs to a different
UTC day or a retained proof is unresolved. A current ledger/REST match does
not by itself prove the service is running or that on-chain recovery is complete.

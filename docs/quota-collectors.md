# Quota sources and historical storage

Agent Quota owns collection, quota history, forecasting and warnings. Configure
its service separately and set `capacity.quota.socket` in Run as described in
[provider configuration](provider-config-v2.md#quota-service). Run reads the
resulting snapshot for selection and public quota views. An unavailable result
is unknown; a partial result shows available facts with an incomplete marker
and leaves missing quota unknown. Run does not invoke configured collector
commands or send native quota failures to Agent Quota.

The former `agent-run capacity collect` and `capacity launchd` commands and
Run's old collectors and Linux ingest are no longer included in the source or
new releases. Existing provider `limits_source` and `collector` fields remain
valid configuration and historical migration inputs; Run does not execute
their commands to obtain current quota. Existing installations and schedules
are not changed implicitly. Migrating and qualifying a live collection setup
requires a separate operational step.

Historical Run tables, their migrations and low-level storage operations remain
for compatibility. The old history is not imported into Agent Quota and does
not supply current Run quota reads or selection. Run's local capacity revision
also protects account registry, cooldown and reservation changes, so it is not
interchangeable with Quota's source revision.

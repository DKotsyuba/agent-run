# Process identity

An owned process is identified by its PID and `psutil.Process(pid).create_time()`.
`alive`, `dead`, `reused`, `unknown`, and `denied` are distinct verdicts. Missing
legacy birth evidence is `unknown`, never `dead`; an unknown or reused PID is
never signalled. Command text remains diagnostic only. Tests inject `psutil.Process`
at this helper seam. The API coordinator persists its own birth time with the
bounded startup claim, and supervisors persist theirs before READY. Legacy rows
without birth evidence can still prove a missing PID dead,
but a present PID remains unknown until the existing deadline or another exact
ownership proof settles it.

Runtime cleanup signals only the process group whose leader/group equality and
stable psutil creation time were verified. Before signalling, it snapshots any
readable descendants by PID and creation time. Group disappearance and the
observed descendant set are reported separately in one `process_cleanup` event:
`scope`, `group_gone`, nullable `descendants_gone`, and `confirmed`. An escaped
descendant is never signalled individually, and group disappearance alone never
claims that the wider tree was contained. Runtime answer/status classification
remains separate from this broader cleanup scope; a surviving original group
still fails closed.

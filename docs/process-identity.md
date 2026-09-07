# Process identity

An owned process is identified by its PID and `psutil.Process(pid).create_time()`.
`alive`, `dead`, `reused`, `unknown`, and `denied` are distinct verdicts. Missing
legacy birth evidence is `unknown`, never `dead`; an unknown or reused PID is
never signalled. Command text remains diagnostic only. Tests inject `psutil.Process`
at this helper seam. The API coordinator persists its own birth time with the
bounded startup claim; supervisors and workflow runners persist theirs before
READY. Legacy rows without birth evidence can still prove a missing PID dead,
but a present PID remains unknown until the existing deadline or another exact
ownership proof settles it.

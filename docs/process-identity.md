# Process identity

An owned process is identified by its PID and `psutil.Process(pid).create_time()`.
`alive`, `dead`, `reused`, `unknown`, and `denied` are distinct verdicts. Missing
legacy birth evidence is `unknown`, never `dead`; an unknown or reused PID is
never signalled. Command text remains diagnostic only. Tests inject `psutil.Process`
at this helper seam; integration code persists birth time before READY.

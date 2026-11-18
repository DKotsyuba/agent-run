//! Manual cost probe for `OwnedProcess::refresh`, the supervisor's 200 ms
//! descendant scan. Run alone so other test threads do not skew the
//! process-wide `/proc/self/io` and CPU counters:
//!
//! ```text
//! cargo nextest run --release -p agent-run-platform --run-ignored only \
//!     --no-capture -E 'test(=refresh_cost::refresh_cost_probe)'
//! ```
#![cfg(target_os = "linux")]

use agent_run_platform::process;
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command},
    time::{Duration, Instant},
};

/// Kills and reaps the fixture group even when the probe panics.
struct Group(Child);
impl Drop for Group {
    fn drop(&mut self) {
        // SAFETY: the fixture is the leader of its own fresh process group (> 1).
        unsafe { libc::kill(-(self.0.id() as i32), libc::SIGKILL) };
        let _ = self.0.wait();
    }
}

/// Reads one cumulative counter from `/proc/self/io`.
fn io_counter(name: &str) -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
        .unwrap()
        .parse()
        .unwrap()
}

/// Returns this process's consumed user plus system CPU time.
fn cpu_time() -> Duration {
    // SAFETY: rusage is plain old data and getrusage only writes it.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: RUSAGE_SELF with a valid out-pointer has no other preconditions.
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) }, 0);
    let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    Duration::from_micros(micros(usage.ru_utime) + micros(usage.ru_stime))
}

#[test]
#[ignore = "timing probe"]
fn refresh_cost_probe() {
    // A small agent-like tree: one group leader with three long-lived children.
    let fixture = Group(
        Command::new("/bin/sh")
            .args(["-c", "sleep 600 & sleep 600 & sleep 600 & wait"])
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let mut owner = process::OwnedProcess::capture(fixture.0.id() as i32);
    let deadline = Instant::now() + Duration::from_secs(5);
    while owner.capture_revision().0 < 4 {
        assert!(Instant::now() < deadline, "fixture children never appeared");
        std::thread::sleep(Duration::from_millis(20));
        owner.refresh();
    }
    let host = process::processes().unwrap().len();

    let rounds = 200u32;
    let (reads, bytes, cpu, wall) = (
        io_counter("syscr"),
        io_counter("rchar"),
        cpu_time(),
        Instant::now(),
    );
    for _ in 0..rounds {
        owner.refresh();
    }
    let wall = wall.elapsed();
    println!(
        "refresh x{rounds} over {host} processes: {:.3} ms wall, {:.3} ms cpu, \
         {} read syscalls, {} bytes read per refresh",
        wall.as_secs_f64() * 1e3 / f64::from(rounds),
        (cpu_time() - cpu).as_secs_f64() * 1e3 / f64::from(rounds),
        (io_counter("syscr") - reads) / u64::from(rounds),
        (io_counter("rchar") - bytes) / u64::from(rounds),
    );

    // Supervisor-like cadence: one refresh every 200 ms for ten seconds.
    let (cpu, wall) = (cpu_time(), Instant::now());
    let mut tick = Instant::now();
    while wall.elapsed() < Duration::from_secs(10) {
        owner.refresh();
        tick += Duration::from_millis(200);
        std::thread::sleep(tick.saturating_duration_since(Instant::now()));
    }
    println!(
        "200 ms supervisor loop: {:.2}% of one CPU",
        (cpu_time() - cpu).as_secs_f64() * 100.0 / wall.elapsed().as_secs_f64()
    );
    assert_eq!(owner.capture_revision().0, 4, "the fixture tree is stable");
}

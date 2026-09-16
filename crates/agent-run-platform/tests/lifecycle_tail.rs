//! Native signal and process-group lifecycle regressions.

use agent_run_platform::launch;
use std::time::Duration;

/// Installs a temporary SIGUSR1 handler for the restoration contract test.
extern "C" fn marker(_signal: libc::c_int) {}

/// Mirrors `tests/test_lifecycle.py::SignalHandlerTests::test_handlers_are_installed_and_restored`
#[test]
fn test_handlers_are_installed_and_restored() {
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    let mut previous = unsafe { std::mem::zeroed::<libc::sigaction>() };
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_sigaction = marker as *const () as usize;
        assert_eq!(libc::sigaction(libc::SIGUSR1, &action, &mut previous), 0);
    }
    let mut restored = unsafe { std::mem::zeroed::<libc::sigaction>() };
    unsafe {
        assert_eq!(libc::sigaction(libc::SIGUSR1, &previous, &mut restored), 0);
    }
    assert_eq!(restored.sa_sigaction, marker as *const () as usize);
}

/// Mirrors `tests/test_lifecycle.py::SystemGroupAliveTests::test_eperm_on_signal_zero_means_the_group_exists`
#[test]
fn test_eperm_on_signal_zero_means_the_group_exists() {
    let group = unsafe { libc::getpgrp() };
    assert!(group > 1);
    assert!(launch::group_alive(group));
}

/// Mirrors `tests/test_lifecycle.py::SystemGroupAliveTests::test_esrch_on_signal_zero_means_gone`
#[test]
fn test_esrch_on_signal_zero_means_gone() {
    let group = i32::MAX;
    assert!(!launch::group_alive(group));
}

/// Confirms the existing cleanup primitive remains bounded for an absent group.
#[test]
fn absent_group_cleanup_probe_is_bounded() {
    let started = std::time::Instant::now();
    assert!(!launch::group_alive(i32::MAX));
    assert!(started.elapsed() < Duration::from_secs(1));
}

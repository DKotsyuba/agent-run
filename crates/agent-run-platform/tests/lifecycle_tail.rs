//! Native signal and process-group lifecycle regressions.

use agent_run_platform::launch;

/// Installs a temporary SIGUSR1 handler for the restoration contract test.
extern "C" fn marker(_signal: libc::c_int) {}

/// Mirrors `tests/test_lifecycle.py::SignalHandlerTests::test_handlers_are_installed_and_restored`
#[test]
fn test_handlers_are_installed_and_restored() {
    // SAFETY: `sigaction` is a C plain-data record initialized before use.
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    // SAFETY: the kernel writes the previous action into this valid record.
    let mut previous = unsafe { std::mem::zeroed::<libc::sigaction>() };
    // SAFETY: the mask is valid and the handler pointer is an ABI-compatible function.
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_sigaction = marker as *const () as usize;
        assert_eq!(libc::sigaction(libc::SIGUSR1, &action, &mut previous), 0);
    }
    // SAFETY: `sigaction` is a C plain-data record initialized before use.
    let mut restored = unsafe { std::mem::zeroed::<libc::sigaction>() };
    // SAFETY: restoring the action uses the valid record returned by the kernel.
    unsafe {
        assert_eq!(libc::sigaction(libc::SIGUSR1, &previous, &mut restored), 0);
    }
    assert_eq!(restored.sa_sigaction, marker as *const () as usize);
}

/// Mirrors `tests/test_lifecycle.py::SystemGroupAliveTests::test_eperm_on_signal_zero_means_the_group_exists`
#[test]
fn test_eperm_on_signal_zero_means_the_group_exists() {
    // SAFETY: getpgrp reads the caller's process group and takes no pointers.
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

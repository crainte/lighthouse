// Native backtrace hook for the intermittent `pure virtual method called` abort.
//
// DuckDB is C++. When a virtual call resolves to a pure-virtual slot (for
// example during destruction or a lifetime race), libstdc++ prints
// "pure virtual method called" and aborts, which tells us nothing about
// which object or thread triggered it.
// Replacing `__cxa_pure_virtual` with a version that prints a backtrace keeps
// the abort (tests must still fail loudly) but makes the next occurrence
// diagnosable.
//
// Loaded through LD_PRELOAD by scripts/ci-test.sh, which sets cargo's runner
// for the host target so only test binaries get it — not cargo, rustc, or the
// C++ build of DuckDB itself. It is test tooling: nothing in the server links
// against it.
#include <execinfo.h>
#include <signal.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

// A marker from inside the executable (not merely the Cargo runner) lets CI
// verify the hook was loaded by the actual test binary. In particular, this
// catches an unexpected Cargo build.target that bypasses the host runner.
__attribute__((constructor)) static void hook_loaded() {
    char exe[4096];
    const auto length = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (length > 0) {
        exe[length] = '\0';
        dprintf(STDERR_FILENO, "[native-hook-loaded] pid=%ld exe=%s\n", (long)getpid(), exe);
    }
}

extern "C" __attribute__((visibility("default"), noreturn)) void __cxa_pure_virtual() {
    void *frames[100];
    int count = backtrace(frames, 100);
    // Best-effort diagnostics while the process is already aborting. Avoid
    // iostreams; backtrace/dprintf themselves may allocate internally.
    dprintf(STDERR_FILENO,
            "[pure-virtual-debug] pid=%ld tid=%ld stack_frames=%d\n",
            (long)getpid(),
            (long)syscall(SYS_gettid),
            count);
    backtrace_symbols_fd(frames, count, STDERR_FILENO);
    abort();
}

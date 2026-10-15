#![cfg(all(unix, feature = "event-stream"))]

use std::thread;
use std::time::Duration;
use std::{ffi::OsStr, fs::File, io, os::unix::ffi::OsStrExt};

use crossterm::event::EventStream;
use futures::StreamExt;
use rustix::{
    pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt},
    stdio::dup2_stdin,
};

#[tokio::test(flavor = "current_thread")]
async fn event_stream_reports_dead_stdin_and_terminates() -> io::Result<()> {
    let master = openpt(OpenptFlags::RDWR)?;
    grantpt(&master)?;
    unlockpt(&master)?;
    let name = ptsname(&master, Vec::new())?;
    let slave = File::options()
        .read(true)
        .write(true)
        .open(OsStr::from_bytes(name.to_bytes()))?;
    dup2_stdin(&slave)?;
    drop(slave);

    let mut stream = EventStream::new();
    let closer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        drop(master);
    });

    let result = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("EventStream did not report the closed stdin");
    let error = result
        .expect("EventStream terminated without reporting the closed stdin")
        .expect_err("EventStream unexpectedly yielded an event");
    assert!(
        error.kind() == io::ErrorKind::UnexpectedEof
            || error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()),
        "PTY hangup must report EOF or EIO: {error}"
    );

    let result = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .expect("EventStream did not terminate after reporting the error");
    assert!(result.is_none());

    closer.join().expect("stdin closer panicked");
    Ok(())
}

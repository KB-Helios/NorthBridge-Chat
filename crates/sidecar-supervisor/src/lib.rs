mod error;
mod log_buffer;
mod model;
mod spec;
mod supervisor;

pub use error::{Result, SidecarError};
pub use model::{LogStream, SidecarLogEntry, SidecarPhase, SidecarSnapshot};
pub use spec::{
    HttpMethod, HttpProbe, IntegrityPolicy, RestartPolicy, ShutdownRequest, SidecarSpec,
};
pub use supervisor::{SidecarSupervisor, verify_executable_integrity};

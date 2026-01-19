use std::error::Error;

pub struct IpResult {
    /// We use Box<dyn Error...> to hold any error type.
    /// Send + Sync is required to work within the threaded Hook system.
    pub err: Option<Box<dyn Error + Send + Sync>>,

    pub ip: String,
}

impl IpResult {
    /// Helper constructor
    pub fn new(ip: String, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { ip, err }
    }

    /// Equivalent to: func (r *IpResult) Error() error
    /// Returns a reference to the error if it exists.
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.err.as_deref()
    }

    /// Equivalent to: func (r *IpResult) GetResult() string
    pub fn get_result(&self) -> String {
        // Go: if r.Err != nil { log... }
        if let Some(e) = &self.err {
            // Note: In a real app, use the `log` crate:
            // log::error!("⚠️ Error executing GetIP: {}", e);
            println!("⚠️ Error executing GetIP: {}", e);
        }

        // Go: return r.IP
        // In Rust, we must clone the String to return an owned value,
        // or return &str if we want to return a reference.
        // Cloning is the closest behavior to Go's string passing here.
        self.ip.clone()
    }
}

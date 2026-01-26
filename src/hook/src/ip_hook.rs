use std::error::Error;

pub struct IpResult {
    pub err: Option<Box<dyn Error + Send + Sync>>,

    pub ip: String,
}

impl IpResult {
    /// Helper constructor
    pub fn new(ip: String, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { ip, err }
    }

    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.err.as_deref()
    }

    pub fn get_result(&self) -> String {
        if let Some(e) = &self.err {
            println!("⚠️ Error executing GetIP: {}", e);
        }

        self.ip.clone()
    }
}

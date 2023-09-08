use dashmap::DashMap;

#[derive(Clone)]
pub struct OpLogEntry {
    pub batch: u32,
    pub id: u32,
    pub r#type: u32,
    pub flags: u32,
    pub total_length: u32,
    pub file_path_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
    pub file_path: String,
    pub meta_data: String,
    pub data: String,
}

pub struct OpLog {
    pub entries: DashMap<String, OpLogEntry>,
}

impl OpLog {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    pub fn add_entry(&self, entry: OpLogEntry) {
        self.entries.insert(entry.file_path.clone(), entry);
    }

    pub fn get_entry(&self, file_path: &str) -> Option<OpLogEntry> {
        let entry = self.entries.get(file_path)?;
        Some(entry.value().clone())
    }

    pub fn remove_entry(&self, file_path: &str) {
        self.entries.remove(file_path);
    }

    pub fn flush_entries(&self, file_paths: Vec<String>) {
        for file_path in file_paths {
            self.entries.remove(&file_path);
        }
    }
}
use microrm::{ConnectionPool, prelude::*};

#[derive(Entity)]
pub struct JobModel {
    #[unique]
    #[key]
    pub job_id: String,
    pub job_name: String,
    pub interval: String,
    pub leeway_seconds: i64,
    pub report_url: Option<String>,
    pub last_run: Option<String>,
    pub last_expected_run: Option<String>,
}

#[derive(Schema, Clone)]
pub struct AppDatabase {
    pub jobs: microrm::IDMap<JobModel>,
}

pub fn create(path: &str) -> (ConnectionPool, AppDatabase) {
    let (cpool, schema) = microrm::ConnectionPool::open::<AppDatabase>(path).unwrap();
    (cpool, schema)
}

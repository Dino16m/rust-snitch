use std::time::Duration;

use paris::info;
use serde::Serialize;
use ureq::Agent;

use crate::{repository::JobRepository, scheduler::Job};

#[derive(Serialize)]
struct SnitchRequest {
    name: String,
    id: String,
    missed_check_in: String,
}

pub struct JobService {
    repository: JobRepository,
    client: Agent,
}

impl JobService {
    pub fn new(repository: JobRepository) -> JobService {
        let client = Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .into();

        JobService { repository, client }
    }

    pub fn snitch(&self, job_id: uuid::Uuid, expected_run: chrono::DateTime<chrono::Utc>) {
        let job = self.repository.get_model(job_id);
        if job.is_err() {
            info!("An error occurred when fetching job: {}", job_id);
            return;
        }
        let job = job.unwrap();
        if job.is_none() {
            info!("Could not find job: {}", job_id);
            return;
        }
        let job = job.unwrap();
        if job.report_url.is_none() {
            info!("Could not find report url for job: {}", job_id);
            return;
        }
        let payload = SnitchRequest {
            name: job.job_name,
            id: job.job_id,
            missed_check_in: expected_run.to_rfc3339(),
        };
        let response = self
            .client
            .post(&job.report_url.unwrap())
            .send_json(&payload);
        match response {
            Ok(response) => {
                info!(
                    "Snitch response for job: {} has status: {}",
                    job_id,
                    response.status()
                );
            }
            Err(e) => {
                info!("Snitch response for job: {} failed: {}", job_id, e);
            }
        }
    }

    pub fn update_job(&self, job: Job) {
        info!("Updating job: {}", job.id());
        if self.repository.update(&job).is_err() {
            info!("Could not update job: {}", job.id());
        }
    }
}

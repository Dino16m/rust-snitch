use std::{collections::HashMap, time::Duration};

use paris::info;
use serde::Serialize;
use ureq::Agent;

use crate::{
    repository::JobRepository,
    scheduler::{Job, JobId},
};

#[derive(Serialize)]
struct SnitchRequest {
    name: String,
    id: String,
    missed_check_in: String,
}

pub struct JobService {
    repository: JobRepository,
    client: Agent,
    snitch_record: HashMap<JobId, chrono::DateTime<chrono::Utc>>,
}

impl JobService {
    pub fn new(repository: JobRepository) -> JobService {
        let client = Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .into();

        JobService {
            repository,
            client,
            snitch_record: HashMap::new(),
        }
    }

    pub fn snitch(&mut self, job_id: uuid::Uuid, expected_run: chrono::DateTime<chrono::Utc>) {
        let job = self.repository.get_model(job_id);
        let Ok(job) = job else {
            info!("An error occurred when fetching job: {}", job_id);
            return;
        };
        let Some(job) = job else {
            info!("Could not find job: {}", job_id);
            return;
        };
        let record = self.snitch_record.get(&job_id);
        if let Some(record) = record {
            if record == &expected_run {
                info!("Snitch message for job: {} has already been sent", job_id);
                return;
            }
        };
        let Some(report_url) = job.report_url else {
            info!("Could not find report url for job: {}", job_id);
            return;
        };
        let payload = SnitchRequest {
            name: job.job_name,
            id: job.job_id,
            missed_check_in: expected_run.to_rfc3339(),
        };
        let response = self.client.post(&report_url).send_json(&payload);
        match response {
            Ok(response) => {
                info!(
                    "Snitch response for job: {} has status: {}",
                    job_id,
                    response.status()
                );
                self.snitch_record.insert(job_id, expected_run);
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

use paris::info;

use crate::{repository::JobRepository, scheduler::Job};

pub struct JobService {
    repository: JobRepository,
}

impl JobService {
    pub fn new(repository: JobRepository) -> JobService {
        JobService { repository }
    }

    pub fn snitch(&self, job_id: uuid::Uuid, expected_run: chrono::DateTime<chrono::Utc>) {
        info!("Job {} should have run at {}", job_id, expected_run);
    }

    pub fn update_job(&self, job: Job) {
        info!("Updating job: {}", job.id());
        if self.repository.update(&job).is_err() {
            info!("Could not update job: {}", job.id());
        }
    }
}

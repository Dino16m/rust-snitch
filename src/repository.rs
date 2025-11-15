use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use microrm::ConnectionPool;
use microrm::prelude::*;

use crate::db::JobModel;
use crate::db::JobModelID;
use crate::{
    db::AppDatabase,
    scheduler::{Job, JobId},
};

#[derive(Debug, Clone)]
pub struct DatabaseError;

impl From<microrm::Error> for DatabaseError {
    fn from(_value: microrm::Error) -> Self {
        return DatabaseError;
    }
}
pub struct JobDTO {
    pub interval: String,
    pub name: String,
    pub leeway_seconds: u64,
    pub report_url: Option<String>,
}

#[derive(Clone)]
pub struct JobRepository {
    schema: AppDatabase,
    conn: ConnectionPool,
}

fn from_model(model: &JobModel) -> Option<Job> {
    let id = uuid::Uuid::from_str(&model.job_id).unwrap();
    let last_run = match &model.last_run {
        Some(last_run) => Some(DateTime::<Utc>::from_str(&last_run).unwrap()),
        None => None,
    };

    let last_expected_run = match &model.last_expected_run {
        Some(last_expected_run) => Some(DateTime::<Utc>::from_str(&last_expected_run).unwrap()),
        None => None,
    };
    Job::new(
        id,
        &model.interval,
        model.leeway_seconds as u64,
        last_run,
        last_expected_run,
    )
}

impl JobRepository {
    pub fn new(schema: AppDatabase, conn: ConnectionPool) -> JobRepository {
        JobRepository { schema, conn }
    }

    pub fn list_jobs(&self) -> Result<Vec<Job>, DatabaseError> {
        let mut txn = self.conn.start()?;

        let jobs = self.schema.jobs.get(&mut txn);
        match jobs {
            Ok(models) => {
                let jobs = models
                    .into_iter()
                    .map(|j| from_model(&j.wrapped()))
                    .filter(|j| j.is_some())
                    .map(|j| j.unwrap())
                    .collect::<Vec<_>>();
                Ok(jobs)
            }
            Err(_) => return Err(DatabaseError),
        }
    }

    pub fn add_job(&self, dto: &JobDTO) -> Result<Job, DatabaseError> {
        let mut txn = self.conn.start()?;
        let model = JobModel {
            job_id: uuid::Uuid::new_v4().to_string(),
            interval: dto.interval.clone(),
            leeway_seconds: dto.leeway_seconds as i64,
            last_run: None,
            last_expected_run: None,
            job_name: dto.name.clone(),
            report_url: dto.report_url.clone(),
        };
        let job = from_model(&model);
        match job {
            Some(job) => {
                let _ = self.schema.jobs.insert(&mut txn, model)?;
                txn.commit()?;
                return Ok(job);
            }
            None => return Err(DatabaseError),
        }
    }
    pub fn find_job(&self, id: JobId) -> Result<Option<Job>, DatabaseError> {
        let mut txn = self.conn.start()?;
        let model = self.schema.jobs.keyed(id.to_string()).get(&mut txn);
        match model {
            Ok(model) => {
                let job = from_model(&model.unwrap());
                return Ok(job);
            }
            Err(_) => return Err(DatabaseError),
        }
    }

    pub fn update(&self, job: &Job) -> Result<(), DatabaseError> {
        let mut txn = self.conn.start()?;
        let model = self
            .schema
            .jobs
            .keyed(job.id().to_string())
            .first()
            .get(&mut txn)?;
        match model {
            Some(mut m) => {
                m.last_expected_run = Some(job.last_expected_run().to_rfc3339());
                m.last_run = Some(job.last_run().to_rfc3339());
                m.sync(&mut txn)?;
            }
            None => todo!(),
        }
        txn.commit()?;
        return Ok(());
    }

    pub fn remove_job(&self, id: JobId) -> Result<(), DatabaseError> {
        let mut txn = self.conn.start()?;
        self.schema.jobs.keyed(id.to_string()).remove(&mut txn)?;
        txn.commit()?;
        return Ok(());
    }
}

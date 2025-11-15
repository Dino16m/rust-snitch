use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rocket::time::Date;

use crate::scheduler::{Job, JobId};

pub type PunctualityChecker = Receiver<(JobId, Sender<Option<bool>>)>;
pub type JobReceiver = Receiver<Job>;
pub type CheckInReceiver = Receiver<(JobId, DateTime<Utc>)>;

#[derive(Debug, Clone)]
pub struct CommError;

pub struct ReceiverService {
    pub punctuality_rx: PunctualityChecker,
    pub checkin_rx: CheckInReceiver,
    pub removal_rx: Receiver<JobId>,
    pub job_rx: JobReceiver,
}

#[derive(Clone)]
pub struct SenderService {
    punctuality_tx: Sender<(JobId, Sender<Option<bool>>)>,
    check_in_sender: Sender<(JobId, DateTime<Utc>)>,
    job_sender: Sender<Job>,
    removal_sender: Sender<JobId>,
}

impl SenderService {
    pub fn new(
        punctuality_tx: Sender<(JobId, Sender<Option<bool>>)>,
        check_in_sender: Sender<(JobId, DateTime<Utc>)>,
        job_sender: Sender<Job>,
        removal_sender: Sender<JobId>,
    ) -> SenderService {
        SenderService {
            punctuality_tx,
            check_in_sender,
            job_sender,
            removal_sender,
        }
    }

    pub fn is_punctual(&self, id: JobId) -> Option<bool> {
        let (tx, rx) = mpsc::channel::<Option<bool>>();
        if self.punctuality_tx.send((id, tx)).is_err() {
            return None;
        }
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(val) => val,
            Err(_) => None,
        }
    }

    pub fn check_in(&self, id: JobId, time: DateTime<Utc>) -> Result<(), CommError> {
        match self.check_in_sender.send((id, time)) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }

    pub fn add_job(&self, job: Job) -> Result<(), CommError> {
        match self.job_sender.send(job) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }

    pub fn remove_job(&self, id: JobId) -> Result<(), CommError> {
        match self.removal_sender.send(id) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }
}

pub fn create_comm_channels() -> (SenderService, ReceiverService) {
    let (punctuality_tx, punctuality_rx) = mpsc::channel::<(JobId, Sender<Option<bool>>)>();
    let (checkin_tx, checkin_rx) = mpsc::channel::<(JobId, DateTime<Utc>)>();
    let (job_tx, job_rx) = mpsc::channel::<Job>();
    let (removal_tx, removal_rx) = mpsc::channel::<JobId>();
    (
        SenderService::new(punctuality_tx, checkin_tx, job_tx, removal_tx),
        ReceiverService {
            punctuality_rx,
            checkin_rx,
            job_rx,
            removal_rx,
        },
    )
}

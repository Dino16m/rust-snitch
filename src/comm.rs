use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use chrono::{DateTime, Utc};
use paris::info;

use crate::scheduler::{Job, JobId};

pub type PunctualityChecker = Receiver<(JobId, Sender<Option<(bool, Option<DateTime<Utc>>)>>)>;
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
    punctuality_tx: Sender<(JobId, Sender<Option<(bool, Option<DateTime<Utc>>)>>)>,
    check_in_sender: Sender<(JobId, DateTime<Utc>)>,
    job_sender: Sender<Job>,
    removal_sender: Sender<JobId>,
}

impl SenderService {
    pub fn new(
        punctuality_tx: Sender<(JobId, Sender<Option<(bool, Option<DateTime<Utc>>)>>)>,
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

    pub fn is_punctual(&self, id: JobId) -> Option<(bool, Option<DateTime<Utc>>)> {
        let (tx, rx) = mpsc::channel::<Option<(bool, Option<DateTime<Utc>>)>>();
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
    let (punctuality_tx, punctuality_rx) =
        mpsc::channel::<(JobId, Sender<Option<(bool, Option<DateTime<Utc>>)>>)>();
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

pub struct WorkerReceiver {
    pub update_rx: Receiver<Job>,
    pub snitch_rx: Receiver<(JobId, DateTime<Utc>)>,
}

pub struct WorkerSender {
    update_tx: Sender<Job>,
    snitch_tx: Sender<(JobId, DateTime<Utc>)>,
}

impl WorkerSender {
    pub fn snitch(&self, job_id: uuid::Uuid, expected_run: chrono::DateTime<chrono::Utc>) {
        if self.snitch_tx.send((job_id, expected_run)).is_err() {
            info!("Could send snitch message for job: {}", job_id);
        }
    }

    pub fn update_job(&self, job: Job) {
        let job_id = job.id().clone();
        if self.update_tx.send(job).is_err() {
            info!("Could not send update for job: {}", job_id);
        }
    }
}

pub fn create_worker_channels() -> (WorkerSender, WorkerReceiver) {
    let (update_tx, update_rx) = mpsc::channel::<Job>();
    let (snitch_tx, snitch_rx) = mpsc::channel::<(JobId, DateTime<Utc>)>();
    (
        WorkerSender {
            update_tx,
            snitch_tx,
        },
        WorkerReceiver {
            update_rx,
            snitch_rx,
        },
    )
}

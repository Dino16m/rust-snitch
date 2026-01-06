use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use chrono::{DateTime, Utc};
use paris::info;

use crate::scheduler::{Job, JobId};

#[derive(Debug, Clone)]
pub struct CommError;

pub enum SchedulerAction {
    PunctualityCheck(JobId, Sender<Option<(bool, Option<DateTime<Utc>>)>>),
    CheckIn(JobId, DateTime<Utc>),
    RemoveJob(JobId),
    AddJob(Job),
    Tick(DateTime<Utc>),
    RunSnitch,
    RunUpdater,
}

pub struct ReceiverService {
    pub action_rx: Receiver<SchedulerAction>,
    pub action_tx: Sender<SchedulerAction>,
}

#[derive(Clone)]
pub struct SenderService {
    action_tx: Sender<SchedulerAction>,
}

impl SenderService {
    pub fn new(action_tx: Sender<SchedulerAction>) -> SenderService {
        SenderService { action_tx }
    }

    pub fn is_punctual(&self, id: JobId) -> Option<(bool, Option<DateTime<Utc>>)> {
        let (tx, rx) = mpsc::channel::<Option<(bool, Option<DateTime<Utc>>)>>();
        if self
            .action_tx
            .send(SchedulerAction::PunctualityCheck(id, tx))
            .is_err()
        {
            return None;
        }
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(val) => val,
            Err(_) => None,
        }
    }

    pub fn check_in(&self, id: JobId, time: DateTime<Utc>) -> Result<(), CommError> {
        match self.action_tx.send(SchedulerAction::CheckIn(id, time)) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }

    pub fn add_job(&self, job: Job) -> Result<(), CommError> {
        match self.action_tx.send(SchedulerAction::AddJob(job)) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }

    pub fn remove_job(&self, id: JobId) -> Result<(), CommError> {
        match self.action_tx.send(SchedulerAction::RemoveJob(id)) {
            Ok(_) => Ok(()),
            Err(_) => Err(CommError),
        }
    }
}

pub fn create_comm_channels() -> (SenderService, ReceiverService) {
    let (action_tx, action_rx) = mpsc::channel::<SchedulerAction>();
    (
        SenderService::new(action_tx.clone()),
        ReceiverService {
            action_rx,
            action_tx,
        },
    )
}

pub enum Workload {
    UpdateJob(Job),
    Snitch(JobId, DateTime<Utc>),
}

pub struct WorkerReceiver {
    pub workload_rx: Receiver<Workload>,
}

pub struct WorkerSender {
    pub workload_tx: Sender<Workload>,
}

impl WorkerSender {
    pub fn snitch(&self, job_id: uuid::Uuid, expected_run: chrono::DateTime<chrono::Utc>) {
        if self
            .workload_tx
            .send(Workload::Snitch(job_id, expected_run))
            .is_err()
        {
            info!("Could not send snitch message for job: {}", job_id);
        }
    }

    pub fn update_job(&self, job: Job) {
        let job_id = job.id().clone();
        if self.workload_tx.send(Workload::UpdateJob(job)).is_err() {
            info!("Could not send update for job: {}", job_id);
        }
    }
}

pub fn create_worker_channels() -> (WorkerSender, WorkerReceiver) {
    let (workload_tx, workload_rx) = mpsc::channel::<Workload>();
    (WorkerSender { workload_tx }, WorkerReceiver { workload_rx })
}

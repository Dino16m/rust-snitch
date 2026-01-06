use paris::info;

use crate::{comm::WorkerReceiver, comm::Workload, service::JobService};

pub fn run_worker(mut service: JobService, receiver: WorkerReceiver) {
    info!("Starting worker");
    loop {
        let received = receiver.workload_rx.recv();
        match received {
            Ok(workload) => match workload {
                Workload::UpdateJob(job) => service.update_job(job),
                Workload::Snitch(job_id, last_expected_run) => {
                    service.snitch(job_id, last_expected_run)
                }
            },
            Err(_) => break,
        }
    }
}

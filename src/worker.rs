use std::{thread::sleep, time::Duration};

use paris::info;

use crate::{comm::WorkerReceiver, service::JobService};

fn handle_updates(service: &JobService, receiver: &WorkerReceiver) {
    loop {
        let received = receiver.update_rx.recv_timeout(Duration::from_millis(100));
        match received {
            Ok(job) => {
                service.update_job(job);
            }
            Err(_) => break,
        }
    }
}
fn handle_snitches(service: &JobService, receiver: &WorkerReceiver) {
    loop {
        let received = receiver.snitch_rx.recv_timeout(Duration::from_millis(100));
        match received {
            Ok((job_id, last_expected_run)) => {
                service.snitch(job_id, last_expected_run);
            }
            Err(_) => break,
        }
    }
}

pub fn run_worker(service: JobService, receiver: WorkerReceiver) {
    let duration = std::time::Duration::from_secs(100);
    info!("Starting worker");
    loop {
        handle_updates(&service, &receiver);
        handle_snitches(&service, &receiver);

        sleep(duration);
    }
}

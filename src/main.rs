mod comm;
mod scheduler;

use std::thread;

use chrono::{DateTime, Utc};
use scheduler::{Job, JobId, Scheduler};

fn snitch(job_id: JobId, expected_run: DateTime<Utc>) {
    println!("Job {} should have run at {}", job_id, expected_run);
}

fn main() {
    let (sender_service, receiver_service) = comm::create_comm_channels();
    let my_scheduler = Scheduler::new(snitch, vec![], receiver_service);

    let scheduler_thread = thread::spawn(move || {
        let mut my_scheduler = my_scheduler;
        my_scheduler.start();
    });

    let job = Job::new(JobId::new_v4(), "*/5 * * * * * *", 10).unwrap();
    sender_service.add_job(job).unwrap();
    scheduler_thread.join().unwrap()
}

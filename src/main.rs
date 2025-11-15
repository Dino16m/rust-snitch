mod comm;
mod db;
mod repository;
mod scheduler;
mod server;
mod service;

use paris::info;
use rocket::launch;
use scheduler::{Job, JobId, Scheduler};
use service::JobService;
use std::env;
use std::thread;

use crate::server::build_server;

#[launch]
fn application() -> _ {
    let database_url = env::var("DATABASE_URL").unwrap();
    let (cpool, schema) = db::create(&database_url);
    info!("Connected to database");
    let repository = repository::JobRepository::new(schema, cpool);
    let (sender_service, receiver_service) = comm::create_comm_channels();
    let jobs = repository.list_jobs().unwrap();
    let scheduler = Scheduler::new(JobService::new(repository.clone()), jobs, receiver_service);

    let _scheduler_thread = thread::spawn(move || {
        let mut my_scheduler = scheduler;
        my_scheduler.start();
    });

    let rocket_server = build_server(repository, sender_service);

    rocket_server
}

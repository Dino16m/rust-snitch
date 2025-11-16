mod comm;
mod db;
mod repository;
mod scheduler;
mod server;
mod service;
mod worker;

use paris::info;
use rocket::launch;
use scheduler::Scheduler;
use service::JobService;
use std::env;
use std::thread;

use crate::server::build_server;
use crate::worker::run_worker;

#[launch]
fn application() -> _ {
    let database_url = env::var("DATABASE_URL").unwrap();
    let (cpool, schema) = db::create(&database_url);
    info!("Connected to database");
    let repository = repository::JobRepository::new(schema, cpool);
    let (sender_service, receiver_service) = comm::create_comm_channels();
    let (worker_sender, worker_receiver) = comm::create_worker_channels();
    let jobs = repository.list_jobs().unwrap();
    let scheduler = Scheduler::new(worker_sender, jobs, receiver_service);

    let _scheduler_thread = thread::spawn(move || {
        let mut my_scheduler = scheduler;
        my_scheduler.start();
    });

    let service = JobService::new(repository.clone());
    thread::spawn(move || {
        run_worker(service, worker_receiver);
    });

    let rocket_server = build_server(repository, sender_service);

    rocket_server
}

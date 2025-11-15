use chrono::Utc;
use fake::Fake;
use fake::faker::lorem::raw::Word;
use fake::locales::EN;
use rocket::response::status::BadRequest;
use rocket::{
    Build, Rocket, State, delete,
    http::Status,
    post, routes,
    serde::{Deserialize, Serialize, json::Json},
};

use crate::{
    comm::SenderService,
    repository::{JobDTO, JobRepository},
};

#[derive(Deserialize)]
pub struct JobRequest {
    pub schedule: String,
    pub leeway_seconds: u64,
    pub report_url: Option<String>,
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct JobResponse {
    pub id: String,
    pub name: String,
}

#[post("/jobs", data = "<request>")]
pub fn create_job(
    request: Json<JobRequest>,
    repository: &State<JobRepository>,
    sender: &State<SenderService>,
) -> Result<Json<JobResponse>, BadRequest<String>> {
    let name = match &request.name {
        Some(name) => name.clone(),
        None => {
            let name: String = format!(
                "{}-{}",
                Word(EN).fake::<String>(),
                Word(EN).fake::<String>()
            );
            name.clone()
        }
    };
    let job = repository.add_job(&JobDTO {
        interval: request.schedule.clone(),
        leeway_seconds: request.leeway_seconds,
        report_url: request.report_url.clone(),
        name: name.clone(),
    });
    if job.is_err() {
        return Err(BadRequest("Could not create job".to_string()));
    }
    let job_id = job.as_ref().unwrap().id();
    match sender.add_job(job.unwrap()) {
        Ok(_) => Ok(Json(JobResponse {
            id: job_id.to_string(),
            name: name.clone(),
        })),
        Err(_) => Err(BadRequest("Could not create job".to_string())),
    }
}

#[post("/jobs/<id>")]
pub fn check_in(id: String, sender: &State<SenderService>) -> Status {
    match sender.check_in(id.parse().unwrap(), Utc::now()) {
        Ok(_) => Status::NoContent,
        Err(_) => Status::InternalServerError,
    }
}

#[delete("/remove/<id>")]
pub fn remove_job(
    id: String,
    repository: &State<JobRepository>,
    sender: &State<SenderService>,
) -> Status {
    let job_id = uuid::Uuid::try_from(id);
    if job_id.is_err() {
        return Status::BadRequest;
    }
    let job_id = job_id.unwrap();
    if sender.remove_job(job_id.clone()).is_err() {
        return Status::InternalServerError;
    }
    match repository.remove_job(job_id) {
        Ok(_) => Status::NoContent,
        Err(_) => Status::InternalServerError,
    }
}

pub fn build_server(repository: JobRepository, sender: SenderService) -> Rocket<Build> {
    rocket::build()
        .manage(repository)
        .manage(sender)
        .mount("/", routes![check_in, create_job, remove_job])
}

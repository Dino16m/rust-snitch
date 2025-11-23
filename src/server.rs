use chrono::Utc;
use fake::Fake;
use fake::faker::lorem::raw::Word;
use fake::locales::EN;
use rocket::get;
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
pub struct JobCreationResponse {
    pub id: String,
    pub name: String,
}

#[derive(Serialize)]
pub struct JobDetailResponse {
    pub id: String,
    pub name: String,
    pub punctual: Option<bool>,
    pub last_check_in: Option<String>,
}

#[post("/jobs", data = "<request>")]
pub fn create_job(
    request: Json<JobRequest>,
    repository: &State<JobRepository>,
    sender: &State<SenderService>,
) -> Result<Json<JobCreationResponse>, BadRequest<String>> {
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
        Ok(_) => Ok(Json(JobCreationResponse {
            id: job_id.to_string(),
            name: name.clone(),
        })),
        Err(_) => Err(BadRequest("Could not add job".to_string())),
    }
}

#[post("/jobs/<id>")]
pub fn check_in(id: &str, sender: &State<SenderService>) -> Status {
    let job_id = uuid::Uuid::try_from(id);
    if job_id.is_err() {
        return Status::BadRequest;
    }
    let job_id = job_id.unwrap();
    match sender.check_in(job_id, Utc::now()) {
        Ok(_) => Status::NoContent,
        Err(_) => Status::InternalServerError,
    }
}

#[get("/job/<id>")]
pub fn get_job(
    id: &str,
    repository: &State<JobRepository>,
    sender: &State<SenderService>,
) -> Result<Json<JobDetailResponse>, Status> {
    let job_id = uuid::Uuid::try_from(id);
    if job_id.is_err() {
        return Err(Status::BadRequest);
    }
    let job_id = job_id.unwrap();
    let job = match repository.find_job(job_id) {
        Ok(job) => match job {
            Some(job) => job,
            None => return Err(Status::NotFound),
        },
        Err(_) => return Err(Status::InternalServerError),
    };
    let (punctual, last_run) = match sender.is_punctual(job_id) {
        Some(data) => data,
        None => return Err(Status::InternalServerError),
    };
    Ok(Json(JobDetailResponse {
        id: job.id().to_string(),
        name: job.get_name(),
        punctual: Some(punctual),
        last_check_in: match last_run {
            Some(last_run) => Some(last_run.to_rfc3339()),
            None => None,
        },
    }))
}

#[delete("/remove/<id>")]
pub fn remove_job(
    id: &str,
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
    rocket::build().manage(repository).manage(sender).mount(
        "/api/snitch/",
        routes![check_in, create_job, remove_job, get_job],
    )
}

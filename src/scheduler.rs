use chrono::{DateTime, Utc};
use cron::Schedule;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    thread::sleep,
    time::Duration,
};
use uuid::Uuid;

use crate::comm::ReceiverService;

pub type JobId = Uuid;
pub type Snitch = fn(JobId, DateTime<Utc>);
pub type CheckInReceiver = Receiver<JobId>;

pub type PunctualityChecker = Receiver<(JobId, Sender<Option<bool>>)>;
pub type JobReceiver = Receiver<Job>;

pub struct Job {
    id: JobId,
    schedule: Schedule,
    created: DateTime<Utc>,
    last_run: Option<DateTime<Utc>>,
    last_expected_run: Option<DateTime<Utc>>,
    next_run: DateTime<Utc>,
    leeway_seconds: u32,
}

impl Job {
    pub fn new(id: JobId, interval: &str, leeway_seconds: u32) -> Option<Job> {
        let schedule = Schedule::from_str(interval);
        match schedule {
            Ok(schedule) => Some(Job {
                id,
                next_run: schedule.upcoming(Utc).next().unwrap(),
                schedule,
                created: Utc::now(),
                last_run: None,
                last_expected_run: None,
                leeway_seconds,
            }),
            Err(e) => {
                println!("Invalid interval: {}", e);
                None
            }
        }
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn tick(&mut self, now: DateTime<Utc>) {
        self.last_expected_run = Some(self.next_run);
        self.next_run = self.schedule.after(&now).next().unwrap();
    }

    pub fn run(&mut self, now: DateTime<Utc>) {
        self.last_run = Some(now);
        self.tick(now);
    }

    pub fn is_punctual(&self) -> bool {
        if self.last_expected_run.is_none() {
            return true;
        }
        if self.last_run.is_none() {
            return false;
        }
        let diff = self.last_expected_run.unwrap() - self.last_run.unwrap();
        diff.abs().as_seconds_f32() < self.leeway_seconds as f32
    }
}

pub struct Scheduler {
    jobs: HashMap<JobId, Job>,
    snitch: Snitch,
    receiver_service: ReceiverService,
}

impl Scheduler {
    pub fn new(snitch: Snitch, jobs: Vec<Job>, receiver_service: ReceiverService) -> Scheduler {
        let jobs = HashMap::from_iter(jobs.into_iter().map(|j| (j.id(), j)));
        Scheduler {
            jobs,
            snitch,
            receiver_service,
        }
    }

    pub fn add(&mut self, job: Job) {
        println!("Added job: {}", job.id());
        self.jobs.insert(job.id(), job);
    }

    fn tick(&mut self, now: DateTime<Utc>) {
        for job in self.jobs.values_mut() {
            println!("Tick job: {}", job.id());
            job.tick(now);
        }
    }

    pub fn check_in(&mut self, id: JobId) {
        if let Some(job) = self.jobs.get_mut(&id) {
            job.run(Utc::now());
            println!("Checked in job: {}", job.id());
        }
    }

    pub fn is_punctual(&self, id: JobId) -> Option<bool> {
        if let Some(job) = self.jobs.get(&id) {
            Some(job.is_punctual())
        } else {
            None
        }
    }

    fn run_snitch(&self) {
        let snitch = self.snitch;
        for (id, job) in &self.jobs {
            if !job.is_punctual() {
                snitch(*id, job.last_expected_run.unwrap());
            }
        }
    }

    fn check_punctuality(&self) {
        loop {
            let received = self
                .receiver_service
                .punctuality_rx
                .recv_timeout(Duration::from_millis(10));
            match received {
                Ok((id, sender)) => {
                    let punctual = self.is_punctual(id);
                    let _ = sender.send(punctual);
                }
                Err(_) => break,
            }
        }
    }

    fn receive_checkin(&mut self) {
        loop {
            let received = self
                .receiver_service
                .checkin_rx
                .recv_timeout(Duration::from_millis(10));
            match received {
                Ok(id) => self.check_in(id),
                Err(_) => break,
            }
        }
    }

    fn remove_job(&mut self) {
        loop {
            let received = self
                .receiver_service
                .removal_rx
                .recv_timeout(Duration::from_millis(10));
            match received {
                Ok(job_id) => {
                    self.jobs.remove(&job_id);
                    println!("Removed job: {}", job_id);
                }
                Err(_) => break,
            }
        }
    }

    fn receive_job(&mut self) {
        loop {
            let received = self
                .receiver_service
                .job_rx
                .recv_timeout(Duration::from_millis(10));
            match received {
                Ok(job) => {
                    println!("Received job: {}", job.id());
                    self.add(job);
                }
                Err(_) => break,
            }
        }
    }

    pub fn start(&mut self) {
        let sleep_duration = Duration::from_millis(1);
        let mut elapsed_duration = Duration::ZERO;
        let snitch_interval_seconds = Duration::from_mins(5).as_secs();

        loop {
            let now = Utc::now();
            self.tick(now);
            self.check_punctuality();
            self.receive_checkin();
            self.receive_job();
            self.remove_job();

            if elapsed_duration.as_secs() > 0
                && elapsed_duration.as_secs() % snitch_interval_seconds == 0
            {
                self.run_snitch();
            }

            sleep(sleep_duration);
            elapsed_duration += sleep_duration;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn create_test_job() -> Job {
        Job::new(Uuid::new_v4(), "*/5 * * * * * *", 10).unwrap()
    }

    #[test]
    fn should_be_punctual_when_newly_created() {
        let job = create_test_job();
        assert!(job.is_punctual());
    }

    #[test]
    fn should_not_be_punctual_when_ticked_but_not_run() {
        let mut job = create_test_job();
        let now = Utc::now();
        job.tick(now);
        assert!(!job.is_punctual());
    }

    #[test]
    fn should_be_punctual_when_run_exactly_on_time() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time);
        assert!(job.is_punctual());
    }

    #[test]
    fn should_be_punctual_when_run_within_leeway() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        // Run 5 seconds late (within 10 second leeway)
        job.run(expected_time + Duration::seconds(5));
        assert!(job.is_punctual());
    }

    #[test]
    fn should_not_be_punctual_when_run_beyond_leeway() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        // Run 15 seconds late (beyond 10 second leeway)
        job.run(expected_time + Duration::seconds(15));
        assert!(!job.is_punctual());
    }

    #[test]
    fn should_track_punctuality_when_run_multiple_times() {
        let mut job = create_test_job();

        // First run - on time
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time);
        assert!(job.is_punctual(), "Should be punctual after on-time run");

        // Second run - slightly late but within leeway
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time + Duration::seconds(5));
        assert!(job.is_punctual(), "Should be punctual when within leeway");

        // Third run - too late
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time + Duration::seconds(15));
        assert!(
            !job.is_punctual(),
            "Should not be punctual when beyond leeway"
        );
    }
}

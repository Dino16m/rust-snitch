use chrono::{DateTime, Utc};
use cron::Schedule;
use paris::info;
use std::{
    collections::HashMap,
    str::FromStr,
    thread::sleep,
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::comm::{ReceiverService, WorkerSender};

pub type JobId = Uuid;

#[derive(Debug, Clone)]
pub struct Job {
    id: JobId,
    name: String,
    schedule: Schedule,
    last_run: Option<DateTime<Utc>>,
    last_expected_run: Option<DateTime<Utc>>,
    next_run: DateTime<Utc>,
    leeway_seconds: u64,
}

impl Job {
    pub fn new(
        id: JobId,
        interval: &str,
        leeway_seconds: u64,
        name: &str,
        last_run: Option<DateTime<Utc>>,
        last_expected_run: Option<DateTime<Utc>>,
    ) -> Option<Job> {
        let schedule = Schedule::from_str(interval);

        match schedule {
            Ok(schedule) => {
                let Some(next_run) = schedule.upcoming(Utc).next() else {
                    return None;
                };
                Some(Job {
                    id,
                    name: name.to_owned(),
                    next_run,
                    schedule,
                    last_run,
                    last_expected_run,
                    leeway_seconds,
                })
            }
            Err(e) => {
                info!("Invalid interval: {}", e);
                None
            }
        }
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_last_run(&self) -> Option<DateTime<Utc>> {
        self.last_run
    }

    pub fn get_last_expected_run(&self) -> Option<DateTime<Utc>> {
        self.last_expected_run
    }

    fn tick(&mut self, now: DateTime<Utc>) -> bool {
        let mut updated = false;
        if self.next_run <= now {
            self.last_expected_run = Some(self.next_run);
            updated = true;
        }
        if let Some(next_run) = self.schedule.after(&now).next() {
            self.next_run = next_run;
        };

        return updated;
    }

    fn run(&mut self, now: DateTime<Utc>) {
        self.last_run = Some(now);
        self.tick(now);
    }

    fn is_punctual(&self, now: Option<DateTime<Utc>>) -> bool {
        let Some(last_expected_run) = self.last_expected_run else {
            return true;
        };
        let Some(last_run) = self.last_run else {
            return false;
        };
        if last_run > last_expected_run {
            return true;
        }
        let mut within_leeway = false;
        if let Some(now) = now {
            within_leeway =
                (last_expected_run - now).abs().as_seconds_f32() <= self.leeway_seconds as f32;
        }

        let diff = last_expected_run - last_run;
        let ran_recently = diff.abs().as_seconds_f32() < self.leeway_seconds as f32;
        ran_recently || within_leeway
    }
}

pub struct Scheduler {
    jobs: HashMap<JobId, Job>,
    worker_sender: WorkerSender,
    update_queue: HashMap<JobId, Job>,
    receiver_service: ReceiverService,
}

impl Scheduler {
    pub fn new(
        worker_sender: WorkerSender,
        jobs: Vec<Job>,
        receiver_service: ReceiverService,
    ) -> Scheduler {
        let jobs = HashMap::from_iter(jobs.into_iter().map(|j| (j.id(), j)));
        Scheduler {
            jobs,
            worker_sender,
            receiver_service,
            update_queue: HashMap::new(),
        }
    }

    pub fn add(&mut self, job: Job) {
        info!("Added job: {}", job.id());
        self.jobs.insert(job.id(), job);
    }

    fn tick(&mut self, now: DateTime<Utc>) {
        for job in self.jobs.values_mut() {
            let changed = job.tick(now);
            if changed {
                self.update_queue.insert(job.id, job.clone());
            }
        }
    }

    pub fn check_in(&mut self, id: JobId, time: DateTime<Utc>) {
        if let Some(job) = self.jobs.get_mut(&id) {
            job.run(time);
            self.update_queue.insert(job.id, job.clone());
            info!("Checked in job: {}", job.id());
        }
    }

    pub fn is_punctual(&self, id: JobId) -> Option<(bool, Option<DateTime<Utc>>)> {
        let Some(job) = self.jobs.get(&id) else {
            return None;
        };
        let now = Utc::now();
        Some((job.is_punctual(Some(now)), job.last_run))
    }

    fn run_snitch(&self) {
        for (id, job) in &self.jobs {
            info!("Snitching job: {}", id);
            let now = Utc::now();
            if !job.is_punctual(Some(now)) {
                let Some(last_expected_run) = job.last_expected_run else {
                    info!("Job: {} has no last expected run", id);
                    continue;
                };
                if let Some(last_run) = job.last_run {
                    info!(
                        "Job: {} is not punctual: Last expected run {}, last run {}",
                        id,
                        last_expected_run.to_rfc3339(),
                        last_run.to_rfc3339()
                    );
                };
                info!("Job: {} is not punctual", id,);

                self.worker_sender.snitch(*id, last_expected_run);
            }
        }
    }

    fn run_updater(&mut self) {
        let keys: Vec<JobId> = self.update_queue.keys().cloned().collect();
        for key in keys.into_iter() {
            if let Some(job) = self.update_queue.remove(&key) {
                self.worker_sender.update_job(job);
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
                .recv_timeout(Duration::from_millis(2));
            match received {
                Ok((id, time)) => {
                    self.check_in(id, time);
                }
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
                    info!("Removed job: {}", job_id);
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
                    info!("Received job: {}", job.id());
                    self.add(job);
                }
                Err(_) => break,
            }
        }
    }

    pub fn start(&mut self) {
        let sleep_duration = Duration::from_millis(20);
        let snitch_interval = Duration::from_mins(1);
        let update_interval = Duration::from_mins(3);
        info!("Starting scheduler with {} jobs", self.jobs.len());

        let mut timer = Instant::now();
        let mut next_snitch_run = timer + snitch_interval;
        let mut next_update_run = timer + update_interval;

        loop {
            let now = Utc::now();
            self.tick(now);
            self.check_punctuality();
            self.receive_checkin();
            self.receive_job();
            self.remove_job();
            if timer >= next_snitch_run {
                info!("Running snitch");
                self.run_snitch();
                next_snitch_run = timer + snitch_interval;
            }
            if timer >= next_update_run {
                info!("Running updater");
                self.run_updater();
                next_update_run = timer + update_interval;
            }
            sleep(sleep_duration);
            timer += timer.elapsed();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn create_test_job() -> Job {
        Job::new(Uuid::new_v4(), "*/5 * * * * * *", 10, "test", None, None).unwrap()
    }

    #[test]
    fn should_be_punctual_when_newly_created() {
        let job = create_test_job();
        assert!(job.is_punctual(None));
    }

    #[test]
    fn should_not_be_punctual_when_ticked_but_not_run() {
        let mut job = create_test_job();
        let now = Utc::now();
        job.tick(now);
        assert!(!job.is_punctual(None));
    }

    #[test]
    fn should_be_punctual_when_run_exactly_on_time() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time);
        assert!(job.is_punctual(None));
    }

    #[test]
    fn should_be_punctual_when_run_within_leeway() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        // Run 5 seconds late (within 10 second leeway)
        job.run(expected_time + Duration::seconds(14));
        assert!(job.is_punctual(None));
    }

    #[test]
    fn should_not_be_punctual_when_run_beyond_leeway() {
        let mut job = create_test_job();
        let expected_time = job.next_run;
        job.tick(expected_time);
        // Run 15 seconds late (beyond 10 second leeway)
        job.run(expected_time + Duration::seconds(15));
        assert!(!job.is_punctual(None));
    }

    #[test]
    fn should_track_punctuality_when_run_multiple_times() {
        let mut job = create_test_job();

        // First run - on time
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time);
        assert!(
            job.is_punctual(None),
            "Should be punctual after on-time run"
        );

        // Second run - slightly late but within leeway
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time + Duration::seconds(5));
        assert!(
            job.is_punctual(None),
            "Should be punctual when within leeway"
        );

        // Third run - too late
        let expected_time = job.next_run;
        job.tick(expected_time);
        job.run(expected_time + Duration::seconds(15));
        assert!(
            !job.is_punctual(None),
            "Should not be punctual when beyond leeway"
        );
    }
}

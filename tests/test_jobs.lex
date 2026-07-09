# lex-jobs — smoke tests.
#
# Uses an in-memory sqlite DB so each test gets a fresh state and
# no fs-write policy is required. Real production uses Postgres
# (see README); these tests exercise the SQL surface that's common
# between the two.

import "std.sql" as sql

import "std.str" as str

import "std.list" as list

import "std.int" as int

import "std.time" as time

import "../src/jobs" as jobs

# Force a claimed ('running') job's updated_at into the past, simulating a
# worker that claimed it lease_seconds+backdate_extra ago and then died
# before ack/retry/fail — the exact scenario reclaim_stale exists to fix.
fn backdate(db :: Db, id :: Int, seconds_ago :: Int) -> [sql, time] Unit {
  let past := time.now_ms() / 1000 - seconds_ago
  let q := str.join(["UPDATE lex_jobs SET updated_at = ", int.to_str(past), " WHERE id = ", int.to_str(id)], "")
  let __r := sql.exec(db, q, [])
  ()
}

type StatusRow = { status :: Str }

fn status_of(db :: Db, id :: Int) -> [sql] Str {
  let q := str.join(["SELECT status FROM lex_jobs WHERE id = ", int.to_str(id)], "")
  let row_result :: Result[List[StatusRow], SqlError] := sql.query(db, q, [])
  match row_result {
    Err(_) => "?",
    Ok(rows) => match list.head(rows) {
      None => "?",
      Some(r) => r.status,
    },
  }
}

# ---- Fixtures ----------------------------------------------------
# Fresh in-memory DB with the lex_jobs table created.
fn fresh_db() -> [sql, fs_write] Result[Db, Str] {
  match sql.open(":memory:") {
    Err(e) => Err(e.message),
    Ok(db) => match jobs.init_schema(db) {
      Err(m) => Err(m),
      Ok(_) => Ok(db),
    },
  }
}

# Always-Done dispatch for happy-path tests.
fn always_done(_h :: Str, _p :: Str) -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] jobs.WorkOutcome {
  Done
}

# Always-Fail dispatch for failure-path tests.
fn always_fail(_h :: Str, _p :: Str) -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] jobs.WorkOutcome {
  Fail("nope")
}

# Always-Retry dispatch — drives the retry-bookkeeping path.
fn always_retry(_h :: Str, _p :: Str) -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] jobs.WorkOutcome {
  Retry("transient")
}

# ---- Tests -------------------------------------------------------
fn init_schema_is_idempotent() -> [sql, time, fs_write] Result[Unit, Str] {
  match sql.open(":memory:") {
    Err(e) => Err(e.message),
    Ok(db) => match jobs.init_schema(db) {
      Err(m) => Err(str.concat("first init: ", m)),
      Ok(_) => match jobs.init_schema(db) {
        Err(m) => Err(str.concat("second init: ", m)),
        Ok(_) => Ok(()),
      },
    },
  }
}

fn enqueue_returns_increasing_ids() -> [sql, time, fs_write] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(str.concat("first enqueue: ", m)),
      Ok(id1) => match jobs.enqueue(db, "q", "h", "{}") {
        Err(m) => Err(str.concat("second enqueue: ", m)),
        Ok(id2) => if id2 > id1 {
          Ok(())
        } else {
          Err(str.concat("ids not increasing: ", str.concat(int.to_str(id1), str.concat(" -> ", int.to_str(id2)))))
        },
      },
    },
  }
}

fn count_pending_reflects_enqueue() -> [sql, time, fs_write] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.enqueue(db, "q", "h", "{}") {
        Err(m) => Err(m),
        Ok(_) => match jobs.count_pending(db, "q") {
          Err(m) => Err(m),
          Ok(n) => if n == 2 {
            Ok(())
          } else {
            Err(str.concat("expected 2, got ", int.to_str(n)))
          },
        },
      },
    },
  }
}

fn work_one_done_clears_the_queue() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.work_one(db, "q", always_done) {
        Err(m) => Err(str.concat("work_one: ", m)),
        Ok(None) => Err("expected one job processed, got none"),
        Ok(Some(_)) => match jobs.count_pending(db, "q") {
          Err(m) => Err(m),
          Ok(n) => if n == 0 {
            Ok(())
          } else {
            Err(str.concat("expected 0 pending, got ", int.to_str(n)))
          },
        },
      },
    },
  }
}

fn work_one_on_empty_queue_returns_none() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.work_one(db, "q", always_done) {
      Err(m) => Err(m),
      Ok(None) => Ok(()),
      Ok(Some(_)) => Err("expected None on empty queue, got Some"),
    },
  }
}

fn fail_outcome_marks_job_failed_not_pending() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.work_one(db, "q", always_fail) {
        Err(m) => Err(m),
        Ok(_) => match jobs.count_pending(db, "q") {
          Err(m) => Err(m),
          Ok(n) => if n == 0 {
            Ok(())
          } else {
            Err(str.concat("Fail should not leave pending; got ", int.to_str(n)))
          },
        },
      },
    },
  }
}

fn retry_outcome_under_max_attempts_returns_to_pending() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.work_one(db, "q", always_retry) {
        Err(m) => Err(m),
        Ok(_) => match jobs.count_pending(db, "q") {
          Err(m) => Err(m),
          Ok(n) => if n == 1 {
            Ok(())
          } else {
            Err(str.concat("retry under cap should re-pend; got ", int.to_str(n)))
          },
        },
      },
    },
  }
}

fn delayed_job_not_immediately_eligible() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Result[Unit, Str] {
  let opts := { delay_seconds: 3600, max_attempts: 3 }
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue_with(db, "q", "h", "{}", opts) {
      Err(m) => Err(m),
      Ok(_) => match jobs.work_one(db, "q", always_done) {
        Err(m) => Err(m),
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err("delayed job ran early"),
      },
    },
  }
}

# ---- reclaim_stale: crash recovery for orphaned 'running' jobs ---
fn reclaim_stale_requeues_a_job_orphaned_by_a_dead_worker() -> [sql, time, fs_write] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.try_claim(db, "q") {
        Err(m) => Err(m),
        Ok(None) => Err("expected a claimable job"),
        Ok(Some(job)) => {
          let __b := backdate(db, job.id, 100)
          match jobs.reclaim_stale(db, "q", 60) {
            Err(m) => Err(m),
            Ok(n) => if n == 1 {
              if status_of(db, job.id) == "pending" {
                Ok(())
              } else {
                Err(str.concat("expected status=pending after reclaim, got ", status_of(db, job.id)))
              }
            } else {
              Err(str.concat("expected 1 job reclaimed, got ", int.to_str(n)))
            },
          }
        },
      },
    },
  }
}

fn reclaim_stale_leaves_a_live_claim_alone() -> [sql, time, fs_write] Result[Unit, Str] {
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue(db, "q", "h", "{}") {
      Err(m) => Err(m),
      Ok(_) => match jobs.try_claim(db, "q") {
        Err(m) => Err(m),
        Ok(None) => Err("expected a claimable job"),
        Ok(Some(job)) => match jobs.reclaim_stale(db, "q", 60) {
          Err(m) => Err(m),
          Ok(n) => if n == 0 {
            if status_of(db, job.id) == "running" {
              Ok(())
            } else {
              Err(str.concat("expected status still running, got ", status_of(db, job.id)))
            }
          } else {
            Err(str.concat("expected 0 jobs reclaimed (claim is fresh), got ", int.to_str(n)))
          },
        },
      },
    },
  }
}

fn reclaim_stale_fails_a_job_already_at_max_attempts() -> [sql, time, fs_write] Result[Unit, Str] {
  let opts := { delay_seconds: 0, max_attempts: 1 }
  match fresh_db() {
    Err(m) => Err(m),
    Ok(db) => match jobs.enqueue_with(db, "q", "h", "{}", opts) {
      Err(m) => Err(m),
      Ok(_) => match jobs.try_claim(db, "q") {
        Err(m) => Err(m),
        Ok(None) => Err("expected a claimable job"),
        Ok(Some(job)) => {
          let __b := backdate(db, job.id, 100)
          match jobs.reclaim_stale(db, "q", 60) {
            Err(m) => Err(m),
            Ok(_) => if status_of(db, job.id) == "failed" {
              Ok(())
            } else {
              Err(str.concat("expected status=failed (at max_attempts), got ", status_of(db, job.id)))
            },
          }
        },
      },
    },
  }
}

# ---- Suite -------------------------------------------------------
fn suite() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] List[Result[Unit, Str]] {
  [init_schema_is_idempotent(), enqueue_returns_increasing_ids(), count_pending_reflects_enqueue(), work_one_done_clears_the_queue(), work_one_on_empty_queue_returns_none(), fail_outcome_marks_job_failed_not_pending(), retry_outcome_under_max_attempts_returns_to_pending(), delayed_job_not_immediately_eligible(), reclaim_stale_requeues_a_job_orphaned_by_a_dead_worker(), reclaim_stale_leaves_a_live_claim_alone(), reclaim_stale_fails_a_job_already_at_max_attempts()]
}

fn run_all() -> [io, time, crypto, random, sql, fs_read, fs_write, net, concurrent] Unit {
  let failures := list.fold(suite(), 0, fn (n :: Int, r :: Result[Unit, Str]) -> Int {
    match r {
      Ok(_) => n,
      Err(_) => n + 1,
    }
  })
  if failures == 0 {
    ()
  } else {
    let __lex_discard_1 := 1 / 0
    ()
  }
}


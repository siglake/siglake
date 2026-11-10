//! Actual durable selection only. The handoff uses the existing operator
//! default; accepted queue+run lifetime never bounds delivery of its 202.
use super::*;
use crate::query_controls::{
    job_tasks::Child,
    jobs::JobContext,
    runtime::{self, ControlsAuthority},
};

pub(super) async fn submit_controlled_batch(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    resolved: ResolvedLimits,
    ice: Arc<siglake_storage::iceberg::IcebergContext>,
    query: RewrittenQuery,
    authority: Arc<ControlsAuthority>,
) -> Result<Response, ApiError> {
    let slot = authority.jobs.reserve()?;
    state
        .jobs
        .controlled_ready()
        .await
        .map_err(|_| ApiError::service_unavailable("query_job_controls_unavailable"))?;
    let admission = acquire_admission_reservation(
        &state,
        Priority::Batch,
        state.admission.batch_reservation_bytes(),
        None,
    )
    .await?;
    let controls = runtime::current()
        .ok_or_else(|| ApiError::service_unavailable("query_policy_unavailable"))?;
    let context = JobContext::new(state.jobs.owner_id(), &identity, controls)
        .map_err(|_| ApiError::service_unavailable("query_policy_unavailable"))?;
    let reservation = state
        .jobs
        .reserve_controlled_commit()
        .map_err(|_| ApiError::service_unavailable("query_controls_busy"))?;
    let id = reservation.id();
    let tenant = state.job_owner(&identity);
    let history_id = history_buffered::batch_execution_id();
    let history = history_buffered::transfer_batch(id.0);
    if let Some(observation) = &history {
        state.jobs.track_history(id, observation.clone());
    }
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let audit = state.audit.clone();
    let scan = state.query_scan;
    let observer = state.history.as_ref().and_then(|s| s.current());
    let authority_owned = authority.dequeue_authority();
    let jobs = state.jobs.clone();
    // No await after durable acceptance until ownership is retained. Expiry runs
    // on the existing SERVER runtime even if the batch task has never been polled.
    slot.start(move |mut stop| async move {
        let _history=history;
        // Ownership exists BEFORE the first durable write. HTTP cancellation
        // only drops accepted_rx, never this operation or its uncertainty ledger.
        let submitted=if tokio::time::Instant::now()>=controls.deadline {
            None
        }else{
            reservation.writing();
            tokio::select!{biased;
                _=stop.changed()=>None,
                r=tokio::time::timeout_at(controls.deadline,jobs.submit_controlled(id,query.sql.clone(),tenant,history_id,context))=>Some(r),
            }
        };
        let (context,anchor)=match submitted {
            Some(Ok(Ok(accepted))) if tokio::time::Instant::now()<controls.deadline=>accepted,
            other=>{
                let timeout=matches!(other,Some(Err(_))) || tokio::time::Instant::now()>=controls.deadline;
                drop(reservation); // WRITING -> UNCERTAIN, never vanished=rollback.
                drop(admission);
                jobs.clear_abort(id);
                let _=accepted_tx.send(Err(if timeout{runtime::expired()}else{ApiError::service_unavailable("query_job_controls_unavailable")}));
                return;
            }
        };
        // Known durable acceptance survives a lost HTTP response. No await
        // separates acknowledgement, ledger removal, and execution ownership.
        reservation.accepted();
        let _=accepted_tx.send(Ok(context.controls.clone()));
        let (abort,registration)=futures::future::AbortHandle::new_pair();
        jobs.register_abort(id,abort);
        let original=context.deadline(anchor).unwrap_or(anchor);
        let (deadline_tx,mut deadline_rx)=tokio::sync::watch::channel(Some(original));
        let batch_jobs=jobs.clone();
        let batch_history=_history.clone();
        let mut child=Child(jobs.batch_runtime().spawn(async move {
            let _admission=admission;
            let lifecycle=JobLifecycle{jobs:batch_jobs.clone(),id};
            let deadline=BatchDeadline{started:anchor,deadline:original,circuit_breakers:true};
            // This future first polls ON DEQUEUE. Its primary read is not an
            // admission cache or a read performed while the batch task is queued.
            let prepared=async {
                if let Some(observation)=&batch_history {
                    if let (Some(observer),Some(execution))=(&observer,observation.execution_id()) { observer.associate_job(execution,id.0).await; }
                }
                let stored=batch_jobs.controlled_context(id).await.map_err(|_|"query_job_controls_unavailable")?;
                if serde_json::to_value(&stored).ok()!=serde_json::to_value(&context).ok() { return Err("query_job_controls_unavailable"); }
                let (snapshot,budget)=stored.revalidate(&authority_owned).await.map_err(|_|"query_policy_unavailable")?;
                let tightened=anchor.checked_add(budget).ok_or("query_job_controls_unavailable")?.min(original);
                let _=deadline_tx.send(Some(tightened));
                if tokio::time::Instant::now()>=tightened { return Err("query deadline exceeded"); }
                let expiry=stored.controls.accepted_at.checked_add_signed(chrono::Duration::from_std(budget).map_err(|_|"query_job_controls_unavailable")?).ok_or("query_job_controls_unavailable")?;
                let publication=tokio::time::timeout_at(tightened,batch_jobs.start_controlled(id,&stored,&snapshot,expiry)).await
                    .map_err(|_|"query deadline exceeded")?;
                if tokio::time::Instant::now()>=tightened { return Err("query deadline exceeded"); }
                Ok((stored,snapshot,tightened,publication))
            };
            let work=async {
                match deadline.run(prepared).await {
                    Ok(Ok((stored,snapshot,tightened,publication)))=> {
                        let execution_controls=stored.execution_controls(&snapshot,tightened,anchor.into_std());
                        let deadline=BatchDeadline{started:anchor,deadline:tightened,circuit_breakers:true};
                        let mut batch_limits=resolved;
                        batch_limits.timeout=std::time::Duration::from_millis(snapshot.effective_timeout_ms as u64);
                        let approximate=AllowApproximate::for_request(&req);
                        let query_sql=query.sql.clone();
                        runtime::body_scope(execution_controls,drive_controlled_batch_job(
                            BatchJobReporting{lifecycle:&lifecycle,audit:audit.as_ref(),identity:&identity,query_sql:&query_sql,deadline},
                            run_batch_query(ice,query,scan,batch_limits,approximate,Some(&lifecycle),deadline),
                            publication,
                            Some(&deadline_tx),
                        )).await;
                    }
                    Ok(Err(reason))=> {
                        let _=deadline_tx.send(None);
                        let timeout=reason=="query deadline exceeded" || deadline.expired();
                        let status=if timeout{JobStatus::Timeout}else{JobStatus::Failed};
                        let _=persist_terminal_state(&batch_jobs,id,status,true,||batch_jobs.finish_failed(id,reason.into(),timeout)).await;
                    }
                    Err(BudgetSpent)=>{let _=deadline_tx.send(None);persist_timeout(&batch_jobs,id).await;},
                }
            };
            let _=futures::future::Abortable::new(work,registration).await;
        }));
        let mut expiry=Some(original);
        let (timed_out,joined)=loop {
            let timer=async {match expiry {Some(at)=>tokio::time::sleep_until(at).await,None=>std::future::pending::<()>().await}};
            tokio::select! {
                biased;
                _=stop.changed()=>break (false,false),
                _=&mut child.0=>break (false,true),
                _=timer=>{
                    // A completed query may be in separately bounded terminal
                    // cleanup. Read its phase before acting on a stale timer.
                    if deadline_rx.borrow().is_none(){expiry=None;}else{break (true,false);}
                }
                changed=deadline_rx.changed()=> {
                    if changed.is_ok() {expiry=match (expiry,*deadline_rx.borrow_and_update()){(Some(a),Some(b))=>Some(a.min(b)),_=>None};}
                    else { // Closed means child is returning; no busy-loop.
                        let result=tokio::select! {biased; _=stop.changed()=>(false,false),_=&mut child.0=>(false,true)};
                        break result;
                    }
                }
            }
        };
        if !joined {
            child.0.abort();
            if timed_out {
                // Queue expiry must publish even if an unpolled child cannot
                // acknowledge abort yet. Retain BOTH ownership and its join;
                // no execution is polled again for telemetry.
                let _=tokio::join!(&mut child.0,persist_timeout(&jobs,id));
            } else { let _=(&mut child.0).await; }
        }
        jobs.observe_history_after_run(id).await;
        jobs.clear_abort(id);
    });
    let public = accepted_rx
        .await
        .map_err(|_| ApiError::service_unavailable("query_job_controls_unavailable"))??;
    Ok((
        StatusCode::ACCEPTED,
        Json(crate::openapi_dto::BatchSubmitResponse {
            controls: Some(public),
            job_id: id.to_string(),
            status_url: format!("/api/v1/jobs/{id}"),
            result_url: format!("/api/v1/jobs/{id}/result"),
            priority: "batch".into(),
        }),
    )
        .into_response())
}
/// Unlike the legacy driver, a failed running publication is NOT permission
/// to scan. Keep this explicit gate even if the store returns an immediate Err.
async fn drive_controlled_batch_job(
    reporting: BatchJobReporting<'_>,
    run: impl std::future::Future<Output = BatchOutcome>,
    publication: anyhow::Result<CompletionOutcome>,
    finished: Option<&tokio::sync::watch::Sender<Option<tokio::time::Instant>>>,
) {
    let end = || {
        if let Some(tx) = finished {
            let _ = tx.send(None);
        }
    };
    match publication {
        Ok(CompletionOutcome::Applied {
            status: JobStatus::Running,
        }) => {
            let deadline = reporting.deadline;
            let protected = async {
                let outcome = if deadline.expired() {
                    drop(run);
                    BatchOutcome::Timeout(None)
                } else {
                    let outcome = run.await;
                    if deadline.expired() {
                        match outcome {
                            BatchOutcome::Ok(_, cost) => BatchOutcome::Timeout(Some(cost)),
                            BatchOutcome::Failed(_, cost) | BatchOutcome::Timeout(cost) => {
                                BatchOutcome::Timeout(cost)
                            }
                        }
                    } else {
                        outcome
                    }
                };
                end(); // Execution/source guards have dropped; cleanup is separate.
                outcome
            };
            drive_batch_job_after_running_publication(
                reporting,
                protected,
                Ok(Ok(CompletionOutcome::Applied {
                    status: JobStatus::Running,
                })),
            )
            .await;
        }
        Ok(refused) => {
            drop(run);
            end();
            report_refused_batch_start(
                reporting.audit,
                reporting.identity,
                reporting.query_sql,
                reporting.deadline.elapsed().as_millis() as i64,
                refused,
            );
        }
        Err(_) => {
            drop(run);
            end();
            let jobs = &reporting.lifecycle.jobs;
            let id = reporting.lifecycle.id;
            let _ = persist_terminal_state(jobs, id, JobStatus::Failed, true, || {
                jobs.finish_failed(id, "query_job_controls_unavailable".into(), false)
            })
            .await;
        }
    }
}
async fn persist_timeout(jobs: &crate::jobs::JobStore, id: crate::jobs::JobId) {
    let _ = persist_terminal_state(jobs, id, JobStatus::Timeout, true, || {
        jobs.finish_failed(id, "query exceeded the timeout".into(), true)
    })
    .await;
}
#[cfg(test)]
#[path = "sql_controlled_jobs/tests.rs"]
mod tests;

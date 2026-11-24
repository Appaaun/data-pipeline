-- description: Latest silver outcome per source (volumes, quarantine rate) - the data observability model
WITH latest AS (
    SELECT *, row_number() OVER (PARTITION BY source ORDER BY started_at DESC) AS rn
    FROM meta.stage_runs
    WHERE stage = 'silver' AND status = 'success'
)
SELECT
    source,
    run_id,
    started_at                                              AS last_success_at,
    rows_in,
    rows_out,
    rows_quarantined,
    round(100.0 * rows_quarantined / nullif(rows_in, 0), 2) AS quarantine_pct,
    rows_warn,
    duplicates_dropped
FROM latest
WHERE rn = 1
ORDER BY source

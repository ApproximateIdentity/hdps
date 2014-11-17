/* Build 'calendar year' dimension table. */
INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    YEAR(cp.cohort_start_date) AS covariate_id,
    1 AS covariate_count
FROM #cohort_person cp
;

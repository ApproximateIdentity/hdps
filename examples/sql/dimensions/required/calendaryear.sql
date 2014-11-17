/* Build 'calendar year' dimension table. */
CREATE TABLE #dim (
    person_id bigint,
    covariate_id bigint,
    covariate_count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    YEAR(cp.cohort_start_date) AS covariate_id,
    1 AS covariate_count
FROM #cohort_person cp
;

/* Build age dimension table. */
INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    FLOOR((YEAR(cp.cohort_start_date) - p.year_of_birth) / 10)
        AS covariate_id,
    1 AS covariate_count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.person p
        ON cp.person_id = p.person_id
;

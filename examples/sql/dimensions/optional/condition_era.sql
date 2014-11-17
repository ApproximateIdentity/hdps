/* Build condition era dimension table. */
INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    ce.condition_concept_id as covariate_id,
    COUNT(ce.condition_concept_id) as covariate_count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.condition_era ce
        ON cp.person_id = ce.person_id
WHERE
    cp.cohort_start_date < ce.condition_era_start_date
    AND ce.condition_era_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    ce.condition_concept_id
;

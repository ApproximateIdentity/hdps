/* Build procedure occurence dimension table. */
CREATE TABLE #dim (
    person_id bigint,
    covariate_id bigint,
    covariate_count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    po.procedure_occurrence_id as covariate_id,
    COUNT(po.procedure_occurrence_id) as covariate_count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.procedure_occurrence po
        ON cp.person_id = po.person_id
WHERE
    cp.cohort_start_date < po.procedure_date
    AND po.procedure_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    po.procedure_occurrence_id
;

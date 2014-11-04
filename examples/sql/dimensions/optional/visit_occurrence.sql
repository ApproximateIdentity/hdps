/* Build visit occurence dimension table. */
CREATE TABLE #dim (
    person_id bigint,
    covariate_id bigint,
    covariate_count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    vo.visit_occurrence_id as covariate_id,
    COUNT(vo.visit_occurrence_id) as covariate_count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.visit_occurrence vo
        ON cp.person_id = vo.person_id
WHERE
    cp.cohort_start_date < vo.visit_start_date
    AND vo.visit_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    vo.visit_occurrence_id
;

/* Build drug era dimension table. */
CREATE TABLE #dim (
    person_id bigint,
    covariate_id bigint,
    covariate_count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    de.drug_concept_id as covariate_id,
    COUNT(de.drug_concept_id) as covariate_count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.drug_era de
        ON cp.person_id = de.person_id
WHERE
    cp.cohort_start_date < de.drug_era_start_date
    AND de.drug_era_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    de.drug_concept_id
;

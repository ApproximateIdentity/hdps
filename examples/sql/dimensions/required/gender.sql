/* Build gender dimension table. */
INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    p.gender_concept_id AS covariate_id,
    1 AS covariate_count
FROM
    #cohort_person cp INNER JOIN @cdm_schema.dbo.person p
        ON cp.person_id = p.person_id
;

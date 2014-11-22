/* Build condition era dimension table. */
INSERT INTO #dim
SELECT
    cp.person_id,
    SUBSTRING(sm.source_code FROM 1 FOR 3) AS covariate_id,
    COUNT(SUBSTRING(sm.source_code FROM 1 FOR 3)) AS covariate_count
    /*COUNT(covariate_id) AS covariate_count*/
FROM
    #cohort_person cp INNER JOIN @cdm_schema.dbo.condition_era ce
        ON cp.person_id = ce.person_id
    INNER JOIN vocabulary.source_to_concept_map sm
        ON ce.condition_concept_id = sm.target_concept_id
    INNER JOIN vocabulary.vocabulary v
        ON sm.source_vocabulary_id = v.vocabulary_id
WHERE
    cp.cohort_start_date < ce.condition_era_start_date
    AND ce.condition_era_start_date <= cp.cohort_end_date
    AND v.vocabulary_name ~* 'ICD-9'
    AND SUBSTRING(sm.source_code FROM 1 FOR 3) ~ '^[0-9]{3}$'
GROUP BY
    cp.person_id,
    covariate_id
;

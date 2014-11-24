/* Build procedure occurrence dimension table. This uses the ICD-9-CM
 * vocabulary. */
INSERT INTO #dim
SELECT
    cp.person_id,
    SUBSTRING(REPLACE(sm.source_code, '.', '') FROM 2 FOR 3) AS covariate_id,
    COUNT(SUBSTRING(REPLACE(sm.source_code, '.', '') FROM 2 FOR 3))
        AS covariate_count
FROM
    #cohort_person cp INNER JOIN @cdm_schema.dbo.procedure_occurrence po
        ON cp.person_id = po.person_id
    INNER JOIN vocabulary.source_to_concept_map sm
        ON po.procedure_concept_id = sm.target_concept_id
    INNER JOIN vocabulary.vocabulary v
        ON sm.source_vocabulary_id = v.vocabulary_id
WHERE
    cp.cohort_start_date < po.procedure_date
    AND po.procedure_date <= cp.cohort_end_date
    AND v.vocabulary_name ~* 'ICD-9-C.*'
    AND sm.source_code ~ '^V[0-9\.]*$'
GROUP BY
    cp.person_id,
    covariate_id
;

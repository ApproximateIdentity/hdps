/* Build race dimension table. Note that in the MSLR_CDM4 and CCAE_CDM4
 * databases, the race information is not available. This means that every
 * person is assigned 0 as a covariate value. However, this will not affect the
 * analysis because this variable will have 0 predictive power.
 */
INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    p.race_concept_id AS covariate_id,
    1 AS covariate_count
FROM
    #cohort_person cp INNER JOIN @cdm_schema.dbo.person p
        ON cp.person_id = p.person_id
;

/* Build gender dimension table. */
CREATE TABLE #dim (
    person_id bigint,
    concept_id bigint,
    count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    p.gender_concept_id AS concept_id,
    1 AS count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.person p
        ON cp.person_id = p.person_id
;

/* These commands are executed after the dim table has been built. */


INSERT INTO #prevalence
SELECT
    covariate_id,
    COUNT(DISTINCT(person_id))
FROM
    #dim
GROUP BY
    covariate_id
;


INSERT INTO prevalent_ids
SELECT
    covariate_id
FROM #prevalence
WHERE person_count >= @minPatients
ORDER BY @(person_count/2 - @numpersons)
LIMIT @topN
;

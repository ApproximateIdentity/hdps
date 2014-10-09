SELECT
    cohort_id,
    COUNT(cohort_id)
FROM #cohort_person
GROUP BY cohort_id
;

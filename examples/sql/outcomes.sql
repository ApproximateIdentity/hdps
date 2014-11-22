/* Authors:   Patrick Ryan, Martijn Schuemie
 * 
 * Parameterized SQL to create outcomes to be used as input in fitting
 * large-scale analytics.
 */

IF OBJECT_ID('cohort_outcome', 'U') IS NOT NULL --This should only do something in Oracle
  drop table cohort_outcome;

IF OBJECT_ID('tempdb..#cohort_outcome', 'U') IS NOT NULL
  drop table #cohort_outcome;

create table #cohort_outcome
(
	row_id bigint,
	cohort_id int,
	person_id bigint,
	outcome_id int,
	time_to_event int
);


IF OBJECT_ID('cohort_excluded_person', 'U') IS NOT NULL --This should only do something in Oracle
  drop table cohort_excluded_person;

IF OBJECT_ID('tempdb..#cohort_excluded_person', 'U') IS NOT NULL
  drop table #cohort_excluded_person;

create table #cohort_excluded_person
(
	row_id bigint,
	cohort_id int,
	person_id bigint,
	outcome_id int
);


INSERT INTO #cohort_outcome (row_id, cohort_id, person_id, outcome_id, time_to_event)
{@outcome_table == 'CONDITION_OCCURRENCE'} ? {
	SELECT cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		/* ca1.ancestor_concept_id AS outcome_id, */
        /* Warning: This is changed from original outcome sql! */
        1 as outcome_id,
		datediff(dd, cp1.cohort_start_date, co1.condition_start_date) AS time_to_event
	FROM #cohort_person cp1
	INNER JOIN 
		@cdm_schema.dbo.condition_occurrence co1
		ON cp1.person_id = co1.person_id
	INNER JOIN (
		SELECT descendant_concept_id,
			ancestor_concept_id
		FROM @cdm_schema.dbo.concept_ancestor
		WHERE ancestor_concept_id IN (@outcome_concept_ids)
		) ca1
		ON co1.condition_concept_id = descendant_concept_id
	WHERE
		co1.condition_type_concept_id IN (@outcome_condition_type_concept_ids)
		AND co1.condition_start_date > cp1.cohort_start_date
		AND co1.condition_start_date <= cp1.observation_period_end_date
	GROUP BY cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		datediff(dd, cp1.cohort_start_date, co1.condition_start_date),
		ca1.ancestor_concept_id
}
{@outcome_table == 'COHORT'} ? {
	SELECT cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		co1.cohort_concept_id AS outcome_id,
		datediff(dd, cp1.cohort_start_date, co1.cohort_start_date) AS time_to_event
	FROM #cohort_person cp1
	INNER JOIN 
		@cdm_schema.dbo.cohort co1
		ON cp1.person_id = co1.person_id
	INNER JOIN (
		SELECT descendant_concept_id,
			ancestor_concept_id
		FROM @cdm_schema.dbo.concept_ancestor
		WHERE ancestor_concept_id IN (@outcome_concept_ids)
		) ca1
		ON co1.condition_concept_id = descendant_concept_id
	WHERE
		co1.cohort_concept_id in (@outcome_concept_ids)
		AND co1.cohort_start_date > cp1.cohort_start_date
		AND co1.cohort_start_Date <= cp1.observation_period_end_date
	GROUP BY cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		datediff(dd, cp1.cohort_start_date, co1.cohort_start_date),
		co1.cohort_concept_id
}
;

	

	
---find people to exclude from each analysis (if outcome occurs prior to index)	
INSERT INTO #cohort_excluded_person (row_id, cohort_id, person_id, outcome_id)
{@outcome_table == 'CONDITION_OCCURRENCE'} ? {
	SELECT DISTINCT cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		ca1.ancestor_concept_id AS outcome_id
	FROM #cohort_person cp1
	INNER JOIN @cdm_schema.dbo.condition_occurrence co1
		ON cp1.person_id = co1.person_id
	INNER JOIN (
		SELECT descendant_concept_id,
			ancestor_concept_id
		FROM @cdm_schema.dbo.concept_ancestor
		WHERE ancestor_concept_id IN (@outcome_concept_ids)
		) ca1
		ON co1.condition_concept_id = descendant_concept_id
	WHERE
		co1.condition_type_concept_id IN (@outcome_condition_type_concept_ids)
		AND co1.condition_start_date < cp1.cohort_start_date
}
{@outcome_table == 'COHORT'} ? {
	SELECT DISTINCT cp1.row_id,
		cp1.cohort_id,
		cp1.person_id,
		co1.cohort_concept_id AS outcome_id
	FROM #cohort_person cp1
	INNER JOIN @cdm_schema.dbo.cohort co1
		ON cp1.person_id = co1.person_id
	WHERE
		co1.cohort_concept_id in (@outcome_concept_ids)
		AND co1.cohort_start_date < cp1.cohort_start_date
}

	
	;

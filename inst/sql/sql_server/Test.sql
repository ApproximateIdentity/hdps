/* Test file. */

{DEFAULT @results_schema = 'MSLR_CDM4'}

USE @results_schema;

/* This should only do something in Oracle */
IF OBJECT_ID('cohort_covariate', 'U') IS NOT NULL
  drop table #cohort_covariate;

IF OBJECT_ID('tempdb..#cohort_covariate', 'U') IS NOT NULL
  drop table #cohort_covariate;

create table #cohort_covariate (
	row_id bigint,
	cohort_id int,
	person_id bigint,
	covariate_id bigint,
	covariate_value float
);

/********************************************************************************


  #####                                       #     #                                       #####   #####  #       
 #     #  ####  #    #  ####  #####  #####    ##   ## ###### ##### #    #  ####  #####     #     # #     # #       
 #       #    # #    # #    # #    #   #      # # # # #        #   #    # #    # #    #    #       #     # #       
 #       #    # ###### #    # #    #   #      #  #  # #####    #   ###### #    # #    #     #####  #     # #       
 #       #    # #    # #    # #####    #      #     # #        #   #    # #    # #    #          # #   # # #       
 #     # #    # #    # #    # #   #    #      #     # #        #   #    # #    # #    #    #     # #    #  #       
  #####   ####  #    #  ####  #    #   #      #     # ######   #   #    #  ####  #####      #####   #### # ####### 

  
  
Authors:   Patrick Ryan, Martijn Schuemie
Last update date:  23 January 2014

Parameterized SQL to create cohorts, covariates, and outcomes datasets to be used as input in fitting large-scale analytics
  
*********************************************************************************/







/********************************************************************************


 ######                                   ######                                                                
 #     # ###### ###### # #    # ######    #     #   ##   #####    ##   #    # ###### ##### ###### #####   ####  
 #     # #      #      # ##   # #         #     #  #  #  #    #  #  #  ##  ## #        #   #      #    # #      
 #     # #####  #####  # # #  # #####     ######  #    # #    # #    # # ## # #####    #   #####  #    #  ####  
 #     # #      #      # #  # # #         #       ###### #####  ###### #    # #        #   #      #####       # 
 #     # #      #      # #   ## #         #       #    # #   #  #    # #    # #        #   #      #   #  #    # 
 ######  ###### #      # #    # ######    #       #    # #    # #    # #    # ######   #   ###### #    #  ####   


*********************************************************************************/

{DEFAULT @cdm_database_schema = 'CDM4_SIM.dbo'} /*cdm_database_schema: @cdm_database_schema*/
{DEFAULT @results_database_schema = 'scratch.dbo'} /*results_database_schema: @results_database_schema*/
{DEFAULT @cdm_database = 'CDM4_SIM'} /*cdm_database: @cdm_database_schema*/
{DEFAULT @results_database = 'scratch'} /*results_database: @results_database_schema*/
{DEFAULT @target_drug_concept_id = ''}  /*target_drug_concept_id: @target_drug_concept_id*/
{DEFAULT @comparator_drug_concept_id = ''} /*comparator_drug_concept_id: @comparator_drug_concept_id*/
{DEFAULT @indication_concept_ids = ''} /*indication_concept_ids: @indication_concept_ids*/
{DEFAULT @washout_window = 183} /*washout_window: @washout_window*/
{DEFAULT @indication_lookback_window = 183} /*indication_lookback_window: @indication_lookback_window*/
{DEFAULT @study_start_date = ''} /*study_start_date: @study_start_date*/
{DEFAULT @study_end_date = ''} /*study_end_date: @study_end_date*/
{DEFAULT @exclusion_concept_ids = ''}  /*exclusion_concept_ids: @exclusion_concept_ids*/
{DEFAULT @outcome_concept_ids = ''}  /*outcome_concept_ids: @outcome_concept_ids*/
{DEFAULT @outcome_condition_type_concept_ids = ''}    /*outcome_condition_type_concept_ids: @outcome_condition_type_concept_ids*/ /*condition type only applies if @outcome_table = condition_occurrence*/

{DEFAULT @exposure_schema = 'CDM4_SIM'} /*exposure_schema: @exposure_schema*/
{DEFAULT @exposure_table = 'drug_era'}  /*exposure_table: @exposure_table*/ /*the table that contains the exposure information (drug_era or COHORT)*/
{DEFAULT @outcome_schema = 'CDM4_SIM'} /*outcome_schema: @outcome_schema*/
{DEFAULT @outcome_table = 'condition_occurrence'}   /*outcome_table: @outcome_table*/ /*the table that contains the outcome information (condition_occurrence or COHORT)*/


{DEFAULT @excluded_covariate_concept_ids = ''} /*excluded_covariate_concept_ids: @excluded_covariate_concept_ids*/
{DEFAULT @delete_covariates_small_count = 100} /*delete_covariates_small_count: @delete_covariates_small_count*/

USE @results_database;


/********************************************************************************


 ###                                                   #######                                    
  #  #    # # ##### #   ##   #      # ###### ######       #      ##   #####  #      ######  ####  
  #  ##   # #   #   #  #  #  #      #     #  #            #     #  #  #    # #      #      #      
  #  # #  # #   #   # #    # #      #    #   #####        #    #    # #####  #      #####   ####  
  #  #  # # #   #   # ###### #      #   #    #            #    ###### #    # #      #           # 
  #  #   ## #   #   # #    # #      #  #     #            #    #    # #    # #      #      #    # 
 ### #    # #   #   # #    # ###### # ###### ######       #    #    # #####  ###### ######  ####  


*********************************************************************************/

IF OBJECT_ID('raw_cohort', 'U') IS NOT NULL --This should only do something in Oracle
  drop table raw_cohort;

IF OBJECT_ID('tempdb..#indicated_cohort', 'U') IS NOT NULL
  drop table #indicated_cohort;
  
IF OBJECT_ID('new_user_cohort', 'U') IS NOT NULL --This should only do something in Oracle
  drop table new_user_cohort;

IF OBJECT_ID('tempdb..#new_user_cohort', 'U') IS NOT NULL
  drop table #new_user_cohort;
  
IF OBJECT_ID('non_overlap_cohort', 'U') IS NOT NULL --This should only do something in Oracle
  drop table non_overlap_cohort;

IF OBJECT_ID('tempdb..#non_overlap_cohort', 'U') IS NOT NULL
  drop table #non_overlap_cohort;

IF OBJECT_ID('cohort_person', 'U') IS NOT NULL --This should only do something in Oracle
  drop table cohort_person;

IF OBJECT_ID('tempdb..#cohort_person', 'U') IS NOT NULL
  drop table #cohort_person;

IF OBJECT_ID('cohort_covariate', 'U') IS NOT NULL --This should only do something in Oracle
  drop table cohort_covariate;

IF OBJECT_ID('tempdb..#cohort_covariate', 'U') IS NOT NULL
  drop table #cohort_covariate;

create table #cohort_covariate
(
  row_id bigint,
  cohort_id int,
	person_id bigint,
	covariate_id bigint,
	covariate_value float
);

IF OBJECT_ID('cohort_covariate_ref', 'U') IS NOT NULL --This should only do something in Oracle
  drop table cohort_covariate_ref;

IF OBJECT_ID('tempdb..#cohort_covariate_ref', 'U') IS NOT NULL
  drop table #cohort_covariate_ref;

create table #cohort_covariate_ref
(
	covariate_id bigint,
	covariate_name varchar(max),
	analysis_id int,
	concept_id int
);


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


/********************************************************************************


  #####                                        #####                                           
 #     # #####  ######   ##   ##### ######    #     #  ####  #    #  ####  #####  #####  ####  
 #       #    # #       #  #    #   #         #       #    # #    # #    # #    #   #   #      
 #       #    # #####  #    #   #   #####     #       #    # ###### #    # #    #   #    ####  
 #       #####  #      ######   #   #         #       #    # #    # #    # #####    #        # 
 #     # #   #  #      #    #   #   #         #     # #    # #    # #    # #   #    #   #    # 
  #####  #    # ###### #    #   #   ######     #####   ####  #    #  ####  #    #   #    #### 


*********************************************************************************/

/* make a table containing new users */
SELECT DISTINCT raw_cohorts.cohort_id,
  raw_cohorts.person_id,
  raw_cohorts.cohort_start_date,
  {@study_end_date != ''} ? {
  CASE WHEN raw_cohorts.cohort_end_date <= CAST('@study_end_date' AS DATE)
		THEN raw_cohorts.cohort_end_date
		ELSE CAST('@study_end_date' AS DATE)
		END
  } : {raw_cohorts.cohort_end_date}
  AS cohort_end_date,
  {@study_end_date != ''} ? {
	CASE WHEN op1.observation_period_end_date <= CAST('@study_end_date' AS DATE)
		THEN op1.observation_period_end_date
		ELSE CAST('@study_end_date' AS DATE)
		END
  } : {op1.observation_period_end_date}
  AS observation_period_end_date
INTO
#new_user_cohort 
FROM (
		SELECT CASE 
				WHEN ca1.ancestor_concept_id = @target_drug_concept_id
					THEN 1
				WHEN ca1.ancestor_concept_id = @comparator_drug_concept_id
					THEN 0
				ELSE - 1
				END AS cohort_id,
			de1.person_id,
			min(de1.drug_era_start_date) AS cohort_start_date,
			min(de1.drug_era_end_date) AS cohort_end_date
		FROM @cdm_database_schema.drug_era de1
		INNER JOIN @cdm_database_schema.concept_ancestor ca1
			ON de1.drug_concept_id = ca1.descendant_concept_id
		WHERE ca1.ancestor_concept_id in (@target_drug_concept_id,@comparator_drug_concept_id)
		GROUP BY ca1.ancestor_concept_id,
			de1.person_id
	) raw_cohorts
INNER JOIN @cdm_database_schema.observation_period op1
	ON raw_cohorts.person_id = op1.person_id
WHERE raw_cohorts.cohort_start_date <= op1.observation_period_end_date
  AND raw_cohorts.cohort_start_date >= dateadd(dd, @washout_window, observation_period_start_date)
  {@study_start_date != ''} ? {AND raw_cohorts.cohort_start_date >= CAST('@study_start_date' AS DATE)}
	{@study_end_date != ''} ? {AND raw_cohorts.cohort_start_date <= CAST('@study_end_date' AS DATE)}
;

{@indication_concept_ids != ''} ? {
/* select only users with the indication */
SELECT DISTINCT cohort_id,
  new_user_cohort.person_id,
  cohort_start_date,
  cohort_end_date,
  observation_period_end_date
INTO
#indicated_cohort 
FROM (
#new_user_cohort new_user_cohort
INNER JOIN (
	SELECT person_id,
		condition_start_date AS indication_date
	FROM @cdm_database_schema.condition_occurrence
	WHERE condition_concept_id IN (
			SELECT descendant_concept_id
			FROM @cdm_database_schema.concept_ancestor
			WHERE ancestor_concept_id IN (@indication_concept_ids)
			)
	) indication
	ON new_user_cohort.person_id = indication.person_id
  AND new_user_cohort.cohort_start_date <= dateadd(dd, @indication_lookback_window, indication_date)
  AND new_user_cohort.cohort_start_date >= indication_date
)
;
}

/* delete persons in both cohorts */
SELECT
	cohort_id,
	new_user_cohort.person_id,
	cohort_start_date,
  cohort_end_date,
	observation_period_end_date
INTO
    #non_overlap_cohort
FROM
{@indication_concept_ids != ''} ? {
    #indicated_cohort new_user_cohort
} : {
    #new_user_cohort new_user_cohort
}
LEFT JOIN (
	SELECT person_id
	FROM (
		SELECT person_id,
			count(cohort_id) AS num_cohorts
		FROM 
      {@indication_concept_ids != ''} ? {
          #indicated_cohort
      } : {
          #new_user_cohort
      }
		GROUP BY person_id
		) t1
	WHERE num_cohorts = 2
	) both_cohorts
	ON new_user_cohort.person_id = both_cohorts.person_id	
WHERE
	both_cohorts.person_id IS NULL
;


/* apply exclusion criteria  */
SELECT non_overlap_cohort.person_id*10+non_overlap_cohort.cohort_id as row_id,
	non_overlap_cohort.cohort_id,
	non_overlap_cohort.person_id,
	non_overlap_cohort.cohort_start_date,
	non_overlap_cohort.cohort_end_date,
	non_overlap_cohort.observation_period_end_date
INTO
  #cohort_person 
FROM #non_overlap_cohort non_overlap_cohort
/*
{@exclusion_concept_ids != ''} ? {
LEFT JOIN (
	SELECT *
	FROM @cdm_database_schema.condition_occurrence co1
	WHERE condition_concept_id IN (
			SELECT descendant_concept_id
			FROM @cdm_database_schema.concept_ancestor
			WHERE ancestor_concept_id IN (@exclusion_concept_ids)
			)
	) exclude_conditions
	ON non_overlap_cohort.person_id = exclude_conditions.person_id
		AND non_overlap_cohort.cohort_start_date > exclude_conditions.condition_start_date
LEFT JOIN (
	SELECT *
	FROM @cdm_database_schema.procedure_occurrence po1
	WHERE procedure_concept_id IN (
			SELECT descendant_concept_id
			FROM @cdm_database_schema.concept_ancestor
			WHERE ancestor_concept_id IN (@exclusion_concept_ids)
			)
	) exclude_procedures
	ON non_overlap_cohort.person_id = exclude_procedures.person_id
		AND non_overlap_cohort.cohort_start_date > exclude_procedures.procedure_date
LEFT JOIN (
	SELECT *
	FROM @cdm_database_schema.drug_exposure de1
	WHERE drug_concept_id IN (
			SELECT descendant_concept_id
			FROM @cdm_database_schema.concept_ancestor
			WHERE ancestor_concept_id IN (@exclusion_concept_ids)
			)
	) exclude_drugs
	ON non_overlap_cohort.person_id = exclude_drugs.person_id
		AND non_overlap_cohort.cohort_start_date > exclude_drugs.drug_exposure_start_date
WHERE
	exclude_conditions.person_id IS NULL
	AND exclude_procedures.person_id IS NULL
	AND exclude_drugs.person_id IS NULL
*/
}
;


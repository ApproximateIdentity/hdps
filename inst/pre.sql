/* This creates the necessary tables to build the dimension data on the
 * server.
 */


IF OBJECT_ID('dim', 'U') IS NOT NULL
    DROP TABLE dim;
IF OBJECT_ID('tempdb..#dim', 'U') IS NOT NULL
    DROP TABLE #dim;

IF OBJECT_ID('prevalence', 'U') IS NOT NULL
    DROP TABLE prevalence;
IF OBJECT_ID('tempdb..#prevalence', 'U') IS NOT NULL
    DROP TABLE #prevalence;

IF OBJECT_ID('prevalent_ids', 'U') IS NOT NULL
    DROP TABLE prevalent_ids;
IF OBJECT_ID('tempdb..#prevalent_ids', 'U') IS NOT NULL
    DROP TABLE #prevalent_ids;


CREATE TABLE #dim (
    person_id bigint,
    covariate_id varchar,
    covariate_count int)
;

CREATE TABLE #prevalence (
    covariate_id varchar,
    person_count int)
;

CREATE TABLE #prevalent_ids (
    covariate_id varchar)
;

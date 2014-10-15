library(hdps)

# Base repository folder.
basedir <- Sys.getenv("HDPSDIR")

# Login info.
password <- Sys.getenv("MYPGPASSWORD")
dbms <- "redshift"
user <- Sys.getenv("USER")
server <- "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven"
schema <- "mslr_cdm4"
port <- "5439"

# Drugs to compare.
Erythromycin = 1746940
Amoxicillin = 1713332

# Condition to check for.
MyocardialInfarction = 35205189

hdps = Hdps$new()
#hdps$toggledebug()

hdps$connect(dbms, user, password, server, port, schema)

hdps$buildCohorts(Erythromycin, Amoxicillin, MyocardialInfarction)

hdps$getCohortSize()

# Build condition era dimension table.
parametrizedSql = "
CREATE TABLE #dim (
    person_id bigint,
    concept_id bigint,
    count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    ce.condition_concept_id as concept_id,
    COUNT(ce.condition_concept_id) as count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.condition_era ce
        ON cp.person_id = ce.person_id
WHERE
    cp.cohort_start_date < ce.condition_era_start_date
    AND ce.condition_era_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    ce.condition_concept_id
;
"
hdps$buildDimension(parametrizedSql)

covariates <- hdps$extractCovariates(cutoff = 50)

# Build drug era dimension table.
parametrizedSql = "
CREATE TABLE #dim (
    person_id bigint,
    concept_id bigint,
    count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    de.drug_concept_id as concept_id,
    COUNT(de.drug_concept_id) as count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.drug_era de
        ON cp.person_id = de.person_id
WHERE
    cp.cohort_start_date < de.drug_era_start_date
    AND de.drug_era_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    de.drug_concept_id
;
"
hdps$buildDimension(parametrizedSql)

new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

# Build visit occurence dimension table.
parametrizedSql = "
CREATE TABLE #dim (
    person_id bigint,
    concept_id bigint,
    count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    vo.visit_occurrence_id as concept_id,
    COUNT(vo.visit_occurrence_id) as count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.visit_occurrence vo
        ON cp.person_id = vo.person_id
WHERE
    cp.cohort_start_date < vo.visit_start_date
    AND vo.visit_start_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    vo.visit_occurrence_id
;
"
hdps$buildDimension(parametrizedSql)

new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)


# TODO: Create the dimensions table one time and then add the truncate table
# portion of the code to the buildDimension() method.

# Build procedure occurence dimension table.
parametrizedSql = "
CREATE TABLE #dim (
    person_id bigint,
    concept_id bigint,
    count int
);

INSERT INTO #dim
SELECT DISTINCT
    cp.person_id,
    po.procedure_occurrence_id as concept_id,
    COUNT(po.procedure_occurrence_id) as count
FROM
    #cohort_person cp INNER JOIN mslr_cdm4.procedure_occurrence po
        ON cp.person_id = po.person_id
WHERE
    cp.cohort_start_date < po.procedure_date
    AND po.procedure_date <= cp.cohort_end_date
GROUP BY
    cp.person_id,
    po.procedure_occurrence_id
;
"
hdps$buildDimension(parametrizedSql)

new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

hdps$disconnect()

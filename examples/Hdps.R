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

# Build conditions dimension table.
parametrizedSql = "
CREATE TABLE #dim (
    person_id bigint,
    concept_id int,
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

new_covariates <- hdps$extractCovariates(cutoff = 50)

hdps$disconnect()

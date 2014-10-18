library(hdps)

# Base repository folder.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")

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

rifaximin = 1735947
Lactulose = 987245

# Condition to check for.
MyocardialInfarction = 35205189

hdps = Hdps$new()
#hdps$toggledebug()

hdps$connect(dbms, user, password, server, port, schema)

hdps$buildCohorts(rifaximin, Lactulose, MyocardialInfarction)
query <- "
SELECT
    person_id as row_id,
    cohort_id as y
FROM #cohort_person
;
"
cohortdata <- hdps$connection$executeforresult(query)

filepath <- file.path(datadir, "cohorts.csv")
write.table(cohortdata, file=filepath, sep="\t", row.names=FALSE)

hdps$getCohortSize()

# Generate and download data.
sqlfiles <- list.files(file.path(sqldir, "dimensions"))
for (filename in sqlfiles) {
    filepath <- file.path(sqldir, "dimensions", filename)
    dimensionname <- file_path_sans_ext(basename(filepath))

    parametrizedSql <- loadLocalSql(filepath)
    hdps$buildDimension(parametrizedSql)

    dimensiondata <- hdps$extractDimensionData()
    filename <- paste(dimensionname, ".csv", sep="")
    filepath <- file.path(datadir, "dimensions", filename)
    write.table(dimensiondata, file=filepath, sep="\t", row.names=FALSE)
}

# Everthing below here is wrong...

covariates <- hdps$extractCovariates()

# Build gender dimension table.
parametrizedSql <- loadLocalSql(paste(dimdir, "gender.sql", sep=""))
hdps$buildDimension(parametrizedSql)
new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

# Build condition era dimension table.
parametrizedSql <- loadLocalSql(paste(dimdir, "condition_era.sql", sep=""))
hdps$buildDimension(parametrizedSql)
new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

# Build drug era dimension table.
parametrizedSql <- loadLocalSql(paste(dimdir, "drug_era.sql", sep=""))
hdps$buildDimension(parametrizedSql)
new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

# Build visit occurence dimension table.
parametrizedSql <- loadLocalSql(paste(dimdir, "visit_occurrence.sql", sep=""))
hdps$buildDimension(parametrizedSql)
new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

# TODO: Create the dimensions table one time and then add the truncate table
# portion of the code to the buildDimension() method.

# Build procedure occurence dimension table.
parametrizedSql <- loadLocalSql(paste(dimdir, "procedure_occurrence.sql", sep=""))
hdps$buildDimension(parametrizedSql)
new_covariates <- hdps$extractCovariates(cutoff = 50)
covariates <- rbind(covariates, new_covariates)

library(Cyclops)
names(covariates) <- c("row_id", "covariate_id", "covariate_value")

query <- "
SELECT
    person_id as row_id,
    cohort_id as y
FROM #cohort_person
;
"
outcomes <- hdps$connection$executeforresult(query)

model <- createCyclopsData(outcomes, covariates)

hdps$disconnect()

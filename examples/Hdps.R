library(hdps)

# Base repository folder.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")

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

if (FALSE) {
hdps$connect(dbms, user, password, server, port, schema)

hdps$buildCohorts(rifaximin, Lactulose, MyocardialInfarction)
#hdps$getCohortSize()
query <- "
SELECT
    person_id,
    cohort_id
FROM #cohort_person
;
"
cohortdata <- hdps$connection$executeforresult(query)

filepath <- file.path(datadir, "cohorts.csv")
write.table(cohortdata, file=filepath, sep="\t", row.names=FALSE)


# Generate and download dimensions data.
filepaths <- list.files(file.path(sqldir, "dimensions"), full.name=TRUE)
for (filepath in filepaths) {
    dimensionname <- file_path_sans_ext(basename(filepath))

    parametrizedSql <- loadLocalSql(filepath)
    hdps$buildDimension(parametrizedSql)

    dimensiondata <- hdps$extractDimensionData()
    filename <- paste(dimensionname, ".csv", sep="")
    filepath <- file.path(datadir, "dimensions", filename)
    write.table(dimensiondata, file=filepath, sep="\t", row.names=FALSE)
}

hdps$disconnect()

# Next build covariates locally.
filepaths <- list.files(file.path(datadir, "dimensions"), full.names=TRUE)
covariates <- hdps$extractCovariates(filepaths)
filepath <- file.path(covariatesdir, "covariates.csv")
write.table(covariates, file=filepath, sep="\t", row.names=FALSE)

}

# Run Cyclops on the data.
filepath <- file.path(covariatesdir, "covariates.csv")
covariates <- read.table(filepath, header=TRUE)
covariates$person_id <- as.integer64(covariates$person_id)
covariates$covariate_id <- as.integer64(covariates$covariate_id)
covariates$covariate_value <- as.integer64(covariates$covariate_value)
names(covariates) <- c("ROW_ID", "COVARIATE_ID", "COVARIATE_VALUE")
covariates <- arrange(covariates, ROW_ID)

filepath <- file.path(datadir, "cohorts.csv")
outcomes <- read.table(filepath, header=TRUE)
outcomes$person_id <- as.integer64(outcomes$person_id)
outcomes$cohort_id <- as.integer(outcomes$cohort_id)
names(outcomes) <- c("ROW_ID", "Y")
outcomes <- arrange(outcomes, ROW_ID)
outcomes$STRATUM_ID <- outcomes$ROW_ID

library(Cyclops)

modelType = "lr",
addIntercept = TRUE
offsetAlreadyOnLogScale = FALSE
sortCovariates = TRUE
makeCovariatesDense = NULL
  
useOffsetCovariate = NULL  
outcomes$STRATUM_ID = outcomes$ROW_ID
outcomes$TIME = 0

library(Cyclops)
library(CohortMethod)

cyclopsData <- createCyclopsData(outcomes, covariates, modelType = 'lr')

ps <- outcomes[,c("Y","ROW_ID")]

prior = prior("laplace")
control <- control(cvType = "auto", cvRepetitions = 2,
                   noiseLevel = "quiet")

cyclopsFit <- fitCyclopsModel(
    cyclopsData, 
    prior = prior,
    control = control)

pred <- predict(cyclopsFit)

library(hdps)

# Base repository folder. You may want to change this.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")

# Login info.
connectionDetails <- list(
    password = Sys.getenv("MYPGPASSWORD"),
    dbms = "redshift",
    user = Sys.getenv("USER"),
    server = "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven",
    schema = "mslr_cdm4",
    port = "5439")

# Cohort details.
rifaximin <- 1735947
Lactulose <- 987245
MyocardialInfarction <- 35205189

cohortDetails <- list(
    drugA = rifaximin,
    drugB = Lactulose,
    indicator = MyocardialInfarction)

cat("Generating data...\n")

#generateDataFromSql(
#    sqldir,
#    datadir,
#    connectionDetails,
#    cohortDetails = cohortDetails,
#    topN = 100)

generateSimulatedData(datadir)

cat("Converting covariates...\n")
generateCovariatesFromData(datadir, covariatesdir, topN = 100,
                           minPatients = 100, topK = 300)


# Run Cyclops
library(Cyclops)

# Function which loads data into format required by Cyclops.
getSparseData <- function(covariatesdir) {
    covariatesfile <- file.path(covariatesdir, "covariates.csv")

    cohortsfile <- file.path(covariatesdir, "cohorts.csv")

    covariates <- read.table(covariatesfile, header = TRUE, sep = '\t',
                             col.names = c("new_person_id",
                             "new_covariate_id", "new_covariate_value"),
                             colClasses = c(new_person_id="numeric",
                             new_covariate_id="numeric",
                             new_covariate_value="numeric"))

    cohorts <- read.table(cohortsfile, header = TRUE, sep = '\t', col.names =
                          c("new_person_id", "cohort_id"), colClasses =
                          c(new_person_id="numeric", cohort_id="numeric"))

    # TODO: This works because it is assumed that the persons are labeled
    # consecutively without any holes. This should probably be enforced through
    # checks somewhere or incomprehensible errors might be thrown.
    X <- sparseMatrix(i = covariates$new_person_id,
                      j = covariates$new_covariate_id,
                      x = covariates$new_covariate_value)

    # TODO: This should already be ordered, but it is essential that it be so.
    # This should probably be enforced with a check somewhere.
    cohorts <- cohorts[order(cohorts$new_person_id),]
    y <- cohorts$cohort_id

    sparseData <- list(X = X, y = y)
    sparseData
}

sparseData <- getSparseData(covariatesdir)

cat("Running Cyclops...\n")
cyclopsData <- createCyclopsDataFrame(y = sparseData$y,
                                      sx = sparseData$X,
                                      modelType = "pr")
cyclopsFit <- fitCyclopsModel(cyclopsData, prior = prior("laplace"))
summary(cyclopsFit)
pred <- predict(cyclopsFit)

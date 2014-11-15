library(hdps)
library(tutils)
library(Cyclops)
library(CohortMethod)

# Base repository folder. You may want to change this.
basedir <- getwd()

# File where all statistics will be logged.
logfile <- file.path(basedir, "log")
logger <- Logger$new(filepath = logfile)

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

drugmap = list(
    rifaximin = 1735947,
    Lactulose = 987245)

outcomemap = list(
    MyocardialInfarction = 35205189
)

# The function that does the analysis.
main <- function() {
    # This for loop will iterate over whatever information we have.
    for (i in 1:10) {
        # Get the current run's information.
        runinfo <- list(
            drugA = 'rifaximin',
            drugB = 'Lactulose',
            indicator = 'MyocardialInfarction')

        logger$log("* * * Next run * * *\n")
        logger$log(string(runinfo))

        cohortDetails <- list(
            drugA = drugmap[runinfo$drugA],
            drugB = drugmap[runinfo$drugB],
            indicator = drugmap[runinfo$indicator])

        cat("Generating data...\n")

        #generateDataFromSql(
        #    sqldir,
        #    datadir,
        #    connectionDetails,
        #    cohortDetails = cohortDetails,
        #    topN = 100)

        #generateSimulatedData(datadir)

        cat("Converting covariates...\n")
        generateCovariatesFromData(datadir, covariatesdir, topN = 100,
                                   topK = 300)

        # Run Cyclops
        sparseData <- getSparseData(covariatesdir)

        cat("Running Cyclops...\n")
        cyclopsData <- createCyclopsDataFrame(y = sparseData$y,
                                              sx = sparseData$X,
                                              modelType = "pr")
        cyclopsFit <- fitCyclopsModel(cyclopsData, prior = prior("laplace"))
        pred <- predict(cyclopsFit)

        propdata <- data.frame(
            TREATMENT = sparseData$y,
            PROPENSITY_SCORE = pred)

        auc <- psAuc(propdata)
        logger$log(string(auc))
    }
}


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


main()

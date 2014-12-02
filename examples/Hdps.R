library(hdps)
library(tutils)
library(Cyclops)
library(CohortMethod)

# Base repository folder. You may want to change this.
basedir <- getwd()

# File where all statistics will be logged.
logfile <- file.path(basedir, "log")
logger <- Logger$new(logfile)

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")
tmpdir <- file.path(basedir, "tmp")

# Login info.
connectionDetails <- list(
    password = Sys.getenv("MYPGPASSWORD"),
    dbms = "redshift",
    user = Sys.getenv("USER"),
    server = "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven",
    #schema = "ccae_cdm4",
    schema = "mslr_cdm4",
    port = "5439")

# Load in study data information.
filepath <- "OMOP2011_drugs_w_indications_comparators_23aug2012.csv"
studydata <- read.csv(
    filepath,
    header = TRUE,
    col.names = c("drugA", "drugAname", "indicator",
                  "indicatorname", "drugB", "drugBname"),
    stringsAsFactors = FALSE)

cohortDetails <- list(
    target_drug_concept_id = NULL,
    comparator_drug_concept_id = NULL,
    indication_concept_ids = NULL,
    washout_window = 183,
    indication_lookback_window = 183,
    study_start_date = "",
    study_end_date = "",
    # TODO: What do these numbers mean?
    exclusion_concept_ids = c(
        4027133,
        4032243,
        4146536,
        2002282,
        2213572,
        2005890,
        43534760,
        21601019),
    exposure_table = "DRUG_ERA")

# Default outcome details.
lowBackPain = 194133

outcomeDetails <- list(
    outcome_concept_ids = lowBackPain,
    # Table containing outcome information. Either 'CONDITION_OCCURRENCE' or
    # 'COHORT'.
    outcome_table = 'CONDITION_OCCURRENCE',
    # Condition type only applies if outcome_table is 'CONDITION_OCCURRENCE'.
    outcome_condition_type_concept_ids = c(
        38000215,
        38000216,
        38000217,
        38000218,
        38000183,
        38000232))

# The function that does the analysis.
main <- function(debug = FALSE) {
    #numrows <- nrow(studydata)
    #debug <- FALSE
    i <- 1
    numrows <- 1
    timer <- Timer$new()
    for (i in 1:numrows) {
        # Get the current run's information.
        studydatum <- studydata[i,]
        #runinfo <- list(
            #target_drug_concept_id = studydatum$drugAname,
            #comparator_drug_concept_id = studydatum$drugBname,
            #indication_concept_ids = studydatum$indicatorname)

        runinfo <- list(
            target_drug_concept_id = 'rifaximin',
            comparator_drug_concept_id = 'Lactulose',
            indication_concept_ids = 'MyocardialInfarction')



        infostring <- sprintf("* * * On run %s out of %s * * *\n", i, numrows)

        cat(infostring)
        logger$log(infostring)
        logger$log(string(runinfo))

        #cohortDetails[['target_drug_concept_id']] <- studydatum$drugA
        #cohortDetails[['comparator_drug_concept_id']] <- studydatum$drugB
        #cohortDetails[['indication_concept_ids']] <- studydatum$indicator
 
        cohortDetails[['target_drug_concept_id']] <- 1735947
        cohortDetails[['comparator_drug_concept_id']] <- 987245
        cohortDetails[['indication_concept_ids']] <- 35205189

        cat("Generating data...\n")

        generateDataFromSql(
            connectionDetails,
            cohortDetails,
            outcomeDetails,
            sqldir,
            datadir,
            tmpdir = tmpdir,
            topN = 100,
            debug = debug)

        # REMOVE
        return()

        # For debug reasons.
        if (debug) {
            next
        }

        #generateSimulatedData(datadir)

        cat("Converting covariates...\n")
        generateCovariatesFromData(datadir, covariatesdir, topN = 100,
                                   topK = 300)

        # REMOVE
        return()
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

        logger$log(sprintf("Number of patients: %s", sparseData$numpersons))
        logger$log(sprintf("Number of covariates: %s\n",
                           sparseData$numcovariates))

        auc <- psAuc(propdata)
        logger$log(string(auc))

        elapsedtime <- sprintf("Elapsed time in run: %s\n", timer$split())
        cat(elapsedtime)
        logger$log(elapsedtime)
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

    numcovariates <- length(unique(covariates$new_covariate_id))
    numpersons <- length(unique(cohorts$new_person_id))

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

    sparseData <- list(X = X, y = y, numpersons = numpersons,
                       numcovariates = numcovariates)
    sparseData
}


main(debug = FALSE)

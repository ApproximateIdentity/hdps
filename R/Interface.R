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


#' @export
getDbHdpsData <- function(
    connectionDetails,
    cdmDatabaseSchema,
    resultsDatabaseSchema,
    targetDrugConceptId,
    comparatorDrugConceptId,
    indicationConceptIds = c(),
    outcomeConceptIds = lowBackPain) {

    # Load in the sql files.
    sqldir <- system.file("sql", package = "hdps")
    basedir <- file.path(getwd(), "hdpsdata")
    for (path in list.files(basedir, full.names = TRUE)) {
        unlink(path, recursive = TRUE)
    }

    # Clean up the folders we'll be neading.
    datadir <- file.path(basedir, "data")
    tmpdir <- file.path(basedir, "tmp")
    covariatesdir <- file.path(basedir, "covariates")
    for (path in c(datadir, tmpdir, covariatesdir)) {
        dir.create(path)
    }

    # Add the final cohort details for the run.
    cohortDetails$target_drug_concept_id = targetDrugConceptId
    cohortDetails$comparator_drug_concept_id = comparatorDrugConceptId
    cohortDetails$indication_concept_ids = indicationConceptIds

    generateDataFromSql(
        connectionDetails,
        cohortDetails,
        outcomeDetails,
        sqldir,
        datadir,
        tmpdir = tmpdir,
        topN = 100,
        minPatients = 50)

    generateCovariatesFromData(
        datadir,
        covariatesdir,
        topN = 100,
        topK = 300)

    cohorts <- read.table(
        file.path(covariatesdir, "cohorts.csv"),
        header = TRUE,
        sep = '\t')
    names(cohorts)[names(cohorts) == "cohort_id"] <- "treatment"
    names(cohorts)[names(cohorts) == "new_person_id"] <- "personId"
    cohorts$rowId <- cohorts$personId

    covariates <- read.table(
        file.path(covariatesdir, "covariates.csv"),
        header = TRUE,
        sep = '\t')

    # Rename the colums as psCreate() expects.
    names(covariates)[names(covariates) == "new_covariate_id"] <- "covariateId"
    names(covariates)[names(covariates) == "new_person_id"] <- "rowId"
    names(covariates)[names(covariates) == "new_covariate_value"] <- "covariateValue"

    # Sort the covariates as Cyclops expects.
    covariates <- covariates[order(covariates$rowId, covariates$covariateId),]

    covariateMap <- read.table(
        file.path(covariatesdir, "covariateMap.csv"),
        header = TRUE,
        sep = '\t')

    covariateRef <- data.frame(
        covariateId = covariateMap$new_covariate_id,
        covariateName = paste(
            "Dimension name: ",
            covariateMap$dim_name,
            ", Old covariate id: ",
            covariateMap$old_covariate_id,
            ", Level: ",
            covariateMap$level,
            sep = "")
    )

    # Read in data from covariatesdir.
    hdpsData <- list(
        cohorts = ff::as.ffdf(cohorts),
        covariates = ff::as.ffdf(covariates),
        covariateRef = ff::as.ffdf(covariateRef))
    class(hdpsData) <- "hdpsData"

    hdpsData
}

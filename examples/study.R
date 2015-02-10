library(tutils)
library(Cyclops)
library(CohortMethod)
library(hdps)

# Base directories.
basedir <- getwd()
tmpdir <- file.path(basedir, "tmp")

# File where all statistics will be logged.
logfile <- file.path(basedir, "log2")
logger <- Logger$new(logfile)

# Login info.
connectionDetails <- createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("USER"),
    password = Sys.getenv("MYPGPASSWORD"), 
    server = "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven",
    #schema = "ccae_cdm4",
    schema = "mslr_cdm4",
    port = "5439")

# Load in study data information.
filepath <- file.path(
    basedir,
    "OMOP2011_drugs_w_indications_comparators_23aug2012v2.csv")
studydata <- read.csv(
    filepath,
    header = TRUE,
    stringsAsFactors = FALSE)
studydata <- studydata[order(-studydata$COHORT_SIZE),]

# Default outcome details.
lowBackPain = 194133

# The function that does the analysis.
main <- function(engine="hdps", offset=NULL, limit=NULL, reverse=FALSE) {
    if (engine == "hdps") {
        getDbData <- getDbHdpsData
    } else if (engine == "cohortmethod") {
        getDbData <- getDbCohortData
    } else {
        stop(sprintf("Engine '%s' not implemented.", engine))
    }
    numrows <- nrow(studydata)
    if (is.null(offset)) {
        offset <- 1
    }
    if (!is.null(limit)) {
        end <- offset + limit - 1
        end <- min(end, numrows)
    } else {
        end <- numrows
    }
    if (reverse) {
        temp <- offset
        offset <- end
        end <- temp
    }
    timer <- Timer$new()
    for (i in offset:end) {
        infostring <- sprintf("* * * On run %s * * *", i)
        cat(paste(infostring, "\n", sep = ""))
        logger$log(infostring)

        # Get the current run's information.
        row <- studydata[i,]

        drug_concept_id <- row$DRUG_CONCEPT_ID
        drug_concept_name <- row$DRUG_CONCEPT_NAME
        comparator_drug_concept_id <- row$COMPARATOR_DRUG_CONCEPT_ID
        comparator_drug_concept_name <- row$COMPARATOR_DRUG_CONCEPT_NAME
        indication_concept_id <- row$INDICATION_CONCEPT_ID
        indication_concept_name <- row$INDICATION_CONCEPT_NAME

        infostring <- paste(
            sprintf("target_drug_concept_id: %s",
                    drug_concept_id),
            sprintf("target_drug_concept_name: %s",
                    drug_concept_name),
            sprintf("comparator_drug_concept_id: %s",
                    comparator_drug_concept_id),
            sprintf("comparator_drug_concept_name: %s",
                    comparator_drug_concept_name),
            sprintf("indication_concept_id: %s",
                    indication_concept_id),
            sprintf("indication_concept_name: %s",
                    indication_concept_name),
            sep = '\n')
        logger$log(infostring)

        # Get SNOMED-CT drug_concept_id from indication.
        drug_indication_concept_ids <- getdrugfromindication(
            connectionDetails,
            indication_concept_id)

        dbdata <- getDbData(
            connectionDetails,
            cdmDatabaseSchema = connectionDetails$schema,
            resultsDatabaseSchema = connectionDetails$schema,
            targetDrugConceptId = drug_concept_id,
            comparatorDrugConceptId = comparator_drug_concept_id,
            indicationConceptIds = drug_indication_concept_ids,
            outcomeConceptIds = lowBackPain)

        # This seems to sometimes throw an error.
        s <- tryCatch({
            list(error = FALSE, summary = summary(dbdata))
        }, error = function(e) {
            list(error = TRUE, message = e$message)
        }) 

        if (s$error) {
            infostring <- paste(
                "Error: General error in summary().",
                sprintf("\terror message: %s", s$message),
                sep = "\n")
            logger$log(infostring)
            cat(paste(infostring, "\n", sep = ""))
            next
        } else {
            # Throw away error information.
            s <- s$summary
        }

        infostring <- paste(
            sprintf("cohort_treated_persons_count: %s", s$treatedPersons),
            sprintf("cohort_comparator_persons_count: %s", s$comparatorPersons),
            sprintf("cohort_covariates_count: %s", s$covariateCount),
            sep = '\n')
        logger$log(infostring)

        # Check to see if there are no covariates.
        if (nrow(dbdata$covariates) == 0) {
            infostring <- "Error: No cohort covariate data. Ending run..."
            logger$log(infostring)
            cat(paste(infostring, "\n", sep = ""))
            next
        }

        # Check to see if there are no treated patients.
        if (s$treatedPersons == 0) {
            infostring <- "Error: Nobody was treated. Ending run..."
            logger$log(infostring)
            cat(paste(infostring, "\n", sep = ""))
            next
        }

        # Check to see if there are no comparator patients.
        if (s$comparatorPersons == 0) {
            infostring <- "Error: Nobody was compared. Ending run..."
            logger$log(infostring)
            cat(paste(infostring, "\n", sep = ""))
            next
        }

        cat("\tComputing auc...\n")
        result <- tryCatch({
            list(error = FALSE, result = createPs(dbdata))
        }, error = function(e) {
            list(error = TRUE, message = e$message)
        }) 

        if (result$error) {
            infostring <- paste(
                "Error: General error in createPs().",
                sprintf("\terror message: %s", result$message),
                sep = "\n")
            logger$log(infostring)
            cat(paste(infostring, "\n", sep = ""))
            next
        } else {
            # Throw away error information.
            result <- result$result
        }

        ps <- result$data
        cyclopsFit <- result$cyclopsFit
        pred <- result$pred

        # Save the propensity score.
        folderpath <- file.path(
            basedir,
            sprintf("results/rank_%s_row_%s", i, row$ROW_ID))
        dir.create(folderpath, showWarnings = FALSE)
        filepath <- file.path(folderpath, "ps.csv")
        write.csv(ps, filepath, row.names = FALSE)

        # Save covariate information.
        covariatemapping <- as.data.frame(dbdata$covariateRef)
        covariatemapping <- data.frame(
            covariate_id = covariatemapping$covariateId,
            covariate_name = covariatemapping$covariateName)
        filepath <- file.path(folderpath, "covariate_mapping.csv")
        write.csv(covariatemapping, filepath, row.names = FALSE)
        
        # Save covariate weight information.
        covariateweights <- data.frame(
            covariate_id = cyclopsFit$estimation$column_label,
            covariate_weight = cyclopsFit$estimation$estimate)
        ordering <- order(covariateweights$covariate_id)
        covariateweights <- covariateweights[ordering,]
        filepath <- file.path(folderpath, "covariate_weights.csv")
        write.csv(covariateweights, filepath, row.names = FALSE)

        # Compute the auc.
        auc <- computePsAuc(ps)

        cat(sprintf("\tauc = %s\n", auc))
        logger$log(sprintf("cohort_auc: %s", auc))

        elapsedtime <- timer$split()
        cat(sprintf("\tElapsed time: %s\n", elapsedtime))
        logger$log(sprintf("cohort_time: %s", elapsedtime))
    }
}


getdrugfromindication <- function(connectionDetails, indication_concept_id) {
    sql <- "
    SELECT DISTINCT
        c2.concept_id
    FROM (
        SELECT
            *
        FROM vocabulary.concept
        WHERE
            concept_id = @indication_concept_id
        ) t1 INNER JOIN vocabulary.concept_relationship cr1
            ON t1.concept_id = cr1.concept_id_1
        INNER JOIN vocabulary.concept c1
            ON cr1.concept_id_2 = c1.concept_id
            AND c1.vocabulary_id = 1
        INNER JOIN vocabulary.concept_ancestor ca1
            ON c1.concept_id = ca1.ancestor_concept_id
        INNER JOIN vocabulary.concept c2
            ON ca1.descendant_concept_id = c2.concept_id
            AND c2.vocabulary_id = 1
    ;
    "

    sql <- renderSql(
        sql = sql,
        indication_concept_id = indication_concept_id)$sql

    conn <- connect(connectionDetails)
    data <- dbGetQuery(conn, sql)
    dbDisconnect(conn)

    data$concept_id
}


createPs <- function(cohortData, 
                     checkSorting = TRUE,
                     outcomeConceptId = NULL, 
                     excludeCovariateIds = NULL,
                     prior = createPrior("laplace", exclude = c(0), useCrossValidation = TRUE),
                     control = createControl(noiseLevel = "silent", cvType = "auto", startingVariance = 0.1)){
  if (is.null(outcomeConceptId) | is.null(cohortData$exclude)){
    cohortSubset <- cohortData$cohorts
    if (is.null(excludeCovariateIds)) {
      covariateSubset <- ffbase::subset.ffdf(cohortData$covariates,covariateId != 1)
    } else {
      excludeCovariateIds <- c(excludeCovariateIds,1)
      t <- in.ff(cohortData$covariates$covariateId, ff::as.ff(excludeCovariateIds))
      covariateSubset <- cohortData$covariates[ffbase::ffwhich(t,t == FALSE),]
    }
  } else {
    t <- in.ff(cohortData$cohorts$rowId ,cohortData$exclude$rowId[cohortData$exclude$outcomeId == outcomeConceptId])
    cohortSubset <- cohortData$cohort[ffbase::ffwhich(t,t == FALSE),]
    t <- in.ff(cohortData$covariates$rowId ,cohortData$exclude$rowId[cohortData$exclude$outcomeId == outcomeConceptId])
    if (is.null(excludeCovariateIds)){
      excludeCovariateIds <- c(1)
    } else {
      excludeCovariateIds <- c(excludeCovariateIds,1)
    }
    t <- t | in.ff(cohortData$covariates$covariateId, ff::as.ff(excludeCovariateIds))
    covariateSubset <- cohortData$covariates[ffbase::ffwhich(t,t == FALSE),]
  }
    #### Change below
  colnames(cohortSubset)[colnames(cohortSubset) == "treatment"] <- "y"
  cyclopsData <- convertToCyclopsData(cohortSubset,
                                      covariateSubset,
                                      modelType="lr",
                                      quiet=TRUE,
                                      checkSorting = checkSorting)
  ps <- ff::as.ram(cohortSubset[,c("y","rowId")])
  cyclopsFit <- fitCyclopsModel(cyclopsData, 
                                prior = prior,
                                control = control)
  pred <- predict(cyclopsFit)
  
  colnames(ps)[colnames(ps) == "y"] <- "treatment"
  data <- data.frame(propensityScore = pred, rowId = as.numeric(attr(pred,"names")))
  data <- merge(data,ps,by="rowId")
  attr(data,"coefficients") <- coef(cyclopsFit)
  attr(data,"priorVariance") <- cyclopsFit$variance[1]
  return(list(data = data, cyclopsFit = cyclopsFit, pred = pred))
}


main()


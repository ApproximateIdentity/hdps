summary.hdpsData <- function(hdpsData) {
    treatedPersons <- sum(hdpsData$cohorts$treatment == 1)
    comparatorPersons <- sum(hdpsData$cohorts$treatment == 0)
    # To handle a bug in ff dealing with an empty ffdf.
    if (nrow(hdpsData$covariates) == 0) {
        covariateCount <- 0
    } else {
        covariateCount <- length(unique(hdpsData$covariates$covariateId))
    }

    s <- list(
        treatedPersons = treatedPersons,
        comparatorPersons = comparatorPersons,
        covariateCount = covariateCount)

    s
}

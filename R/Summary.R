summary.hdpsData <- function(hdpsData) {
    treatedPersons <- sum(hdpsData$cohorts$treatment == 1)
    comparatorPersons <- sum(hdpsData$cohorts$treatment == 0)
    covariateCount <- length(unique(hdpsData$covariates$covariateId))

    s <- list(
        treatedPersons = treatedPersons,
        comparatorPersons = comparatorPersons,
        covariateCount = covariateCount)

    s
}

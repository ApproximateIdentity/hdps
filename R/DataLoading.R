#' @export
getSparseData <- function(covariatesdir) {
    cohortsfile <- file.path(covariatesdir, "cohorts.csv")
    covariatesfile <- file.path(covariatesdir, "covariates.csv")

    cohorts <- read.table(cohortsfile, header=TRUE)
    covariates <- read.table(covariatesfile, header=TRUE)

    numPersons <- max(cohorts$person_id)
    numCovariates <- max(covariates$covariate_id)

    dimensions <- c(numPersons, numCovariates)

    X <- getSparseMatrix(covariates, dimensions)
    y <- arrange(cohorts, person_id)$cohort_id

    sparseData <- list(X = X, y = y)
    sparseData
}


getSparseMatrix <- function(covariates, dimensions) {
    X <- Matrix(0, nrow = dimensions[1], ncol = dimensions[2], sparse = TRUE)
    for (r in 1:nrow(covariates)) {
        row <- covariates[r,]
        i <- row$person_id
        j <- row$covariate_id
        val <- row$covariate_value
        X[i, j] <- val
    }

    X
}

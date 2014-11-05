#' @export
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

#' @export
extractCovariates <- function(filepaths) {
    covariates <- data.frame(person_id=character(),
                             covariate_id=character(),
                             covariate_values=character())

    for (filepath in filepaths) {
        # Now build the covariates locally.
        pre_covariates <- fread(filepath)

        # Get 75% percentiles.
        f <- function(numbers) {
            return(quantile(numbers)[[4]])
        }
        highvals <- pre_covariates[, f(count), by = concept_id]
        highmap <- highvals$V1
        names(highmap) <- highvals$concept_id

        # Get medians.
        f <- function(numbers) {
            return(quantile(numbers)[[3]])
        }
        midvals <- pre_covariates[, f(count), by = concept_id]
        midmap <- midvals$V1
        names(midmap) <- midvals$concept_id

        # Build up the real covariates.
        # This is a hack to make this work right with 64 bit integers.
        # TODO: Somehow avoid this hack.
        person_id = c(as.integer64(0))
        length(person_id) <- 0
        covariate_id = c(as.integer64(0))
        length(covariate_id) <- 0

        numcov = length(pre_covariates$person_id)
        for (i in 1:numcov) {
            row <- pre_covariates[i]
            person <- row$person_id
            concept <- toString(row$concept_id)
            covariate = row$concept_id * 10
            count <- row$count
            if (count >= highmap[[concept]]) {
                covariate <- covariate + 3
            } else if (count >= midmap[[concept]]) {
                covariate <- covariate + 2
            } else {
                covariate <- covariate + 1
            }
            person_id[length(person_id) + 1] <- person
            covariate_id[length(covariate_id) + 1] <- covariate
        }

        covariate_value = rep(1, length(person_id))
        covariates <- rbind(covariates,
                            data.frame(person_id, covariate_id, covariate_value))
    }
    return(covariates)
}

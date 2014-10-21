# TODO: Change 'build' to 'get' in function names. Change 'mapping' to 'map'.
#' @export
buildCovariateRecord <- function(filepaths) {
    # HACK: The initial row is necessary to avoid problems with the integer64
    # data type which result from built-in type coersions within R.
    covariateRecord <- data.frame(
        new_covariate = 0,
        old_covariate = as.integer64.character('0'),
        level = "",
        dim = "",
        stringsAsFactors = FALSE)

    for (filepath in filepaths) {
        dimName <- file_path_sans_ext(basename(filepath))

        oldCov <- unique(fread(filepath)$concept_id)
        numOldCov <- length(oldCov)

        # The three levels correspond to 1, median, and 75% percentile as
        # described in the HDPS algorithm.
        level <- rep(c("l", "m", "h"), numOldCov)

        # Replicate the dimName to fit the column of the data frame.
        dimNames <- rep(dimName, numOldCov * 3)
        
        # The shift of '- 1' on the right is necessary because the initial
        # extra row added to the data frame in the hack above.
        base <- length(covariateRecord$new_covariate) - 1
        offset <- numOldCov * 3
        newCov <- (base + 1) : (base + offset)

        # The old covariates are "tripled"
        cov <- oldCov[1]
        triples <- c(cov, cov, cov)
        for (i in 2:numOldCov) {
            newTriple <- rep(oldCov[i], 3)
            triples <- c(triples, newTriple)
        }
        oldCov <- triples

        newCovariateRecord <- data.frame(
            new_covariate = newCov,
            old_covariate = oldCov,
            level = level,
            dim = dimNames)

        covariateRecord <- rbind(covariateRecord, newCovariateRecord)
    }

    # Remove the initial row and reindex the data frame.
    covariateRecord <- covariateRecord[-1, ]
    rownames(covariateRecord) <- 1:nrow(covariateRecord)
    
    covariateRecord
}


#' @export
buildCovariateMapping <- function(covariateRecord) {
    covMapping <- covariateRecord$new_covariate

    oldCov <- covariateRecord$old_covariate
    level <- covariateRecord$level
    
    covNames <- covariateRecord$dim
    for (i in 1:length(covNames)) {
        cov <- as.character.integer64(oldCov[i])
        name <- paste(covNames[i], cov, level[i], sep="")
        covNames[i] <- name
    }

    names(covMapping) <- covNames
    
    covMapping
}


# TODO: This should probably be combined with buildCovariateMapping so that all
# files are not read through twice, but this is conceptually a little simpler.
#' @export
extractCovariates <- function(filepaths, nameMapping, covariateMapping) {
    covariates <- data.frame()

    for (filepath in filepaths) {
        dimName <- file_path_sans_ext(basename(filepath))
        dimData <- fread(filepath)

        # Get 75% percentiles.
        highvals <- dimData[, seventyFifthPercentile(count), by = concept_id]
        highmap <- highvals$V1
        names(highmap) <- highvals$concept_id

        # Get medians.
        midvals <- dimData[, fiftiethPercentile(count), by = concept_id]
        midmap <- midvals$V1
        names(midmap) <- midvals$concept_id

        # Build up the covariates and person ids.
        person_id = c()
        covariate_id = c()

        numcov = length(dimData$person_id)
        for (i in 1:numcov) {
            row <- dimData[i]

            # Get converted person id.
            person <- row$person_id
            person <- nameMapping[toString(person)]

            # Get converted covariate id.
            concept <- toString(row$concept_id)
            count <- row$count
            if (count >= highmap[[concept]]) {
                level <- "h"
            } else if (count >= midmap[[concept]]) {
                level <- "m"
            } else {
                level <- "l"
            }
            key <- paste(dimName, concept, level, sep="")
            covariate <- covariateMapping[key]

            person_id[length(person_id) + 1] <- person
            covariate_id[length(covariate_id) + 1] <- covariate
        }
        
        # The covariate value is always 1 in HDPS.
        covariate_value = rep(1, length(person_id))

        new_covariates <- data.frame(person_id, covariate_id, covariate_value)
        covariates <- rbind(covariates, new_covariates)
    }

    covariates
}


# TODO: Replace this with "median" function. For some reason using median
# throws errors above.
fiftiethPercentile <- function(numbers) {
    return(quantile(numbers)[[3]])
}


seventyFifthPercentile <- function(numbers) {
    return(quantile(numbers)[[4]])
}

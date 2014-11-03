#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, cutoff=NULL) {
    convertData(datadir, covariatedir, cutoff)
    dummy <- extractCovariates(covariatedir)
}


convertData <- function(datadir, covariatedir, cutoff) {
    pidMap <- convertCohorts(datadir, covariatedir)
    convertOutcomes(datadir, covariatedir, pidMap)
    convertCovariates(datadir, covariatedir, pidMap, cutoff)
}


extractCovariates <- function(covariatedir) {
    print("extractCovariates not implemented")
}


convertOutcomes <- function(datadir, covariatedir, pidMap) {
    infilepath <- file.path(datadir, "outcomes.csv")
    outcomes <- read.table(infilepath, header = TRUE, sep = '\t',
                           col.names = c("old_person_id", "outcome_id"),
                           colClasses = c(old_person_id="character",
                           outcome_id="numeric"))

    outcomes <- merge(pidMap, outcomes)[, c('new_person_id', 'outcome_id')]
    outfilepath <- file.path(covariatesdir, "outcomes.csv")
    write.table(outcomes, file = outfilepath, sep = '\t', row.names = FALSE)
}


convertCohorts <- function(datadir, covariatedir) {
    infilepath <- file.path(datadir, "cohorts.csv")
    cohorts <- read.table(infilepath, header = TRUE, sep = '\t',
                          col.names = c("old_person_id", "cohort_id"),
                          colClasses = c(old_person_id="character",
                          cohort_id="numeric"))

    cohorts$new_person_id <- 1:length(cohorts$old_person_id)

    pidMap <- cohorts[, c('new_person_id', 'old_person_id')]
    outfilepath <- file.path(covariatesdir, "personMap.csv")
    write.table(pidMap, file = outfilepath, sep = '\t', row.names = FALSE)

    cohorts <- cohorts[, c('new_person_id', 'cohort_id')]
    outfilepath <- file.path(covariatesdir, "cohorts.csv")
    write.table(cohorts, file = outfilepath, sep = '\t', row.names = FALSE)

    pidMap
}


convertCovariates <- function(datadir, covariatedir, pidMap, cutoff) {
    if (is.null(cutoff)) {
        cutoff <- 1e10
    }

    reqoutfilepath <- file.path(covariatesdir, "requiredcovariates.csv")
    optoutfilepath <- file.path(covariatesdir, "optionalcovariates.csv")
    covmapoutfilepath <- file.path(covariatesdir, "covariateMap.csv")

    reqdir <- file.path(datadir, "dimensions", "required")
    optdir <- file.path(datadir, "dimensions", "optional")
    reqinpaths <- list.files(reqdir, full.names=TRUE)
    optinpaths <- list.files(optdir, full.names=TRUE)

    # Write header for covariate map.
    header <- sprintf("%s\t%s\t%s\t%s", "new_covariate_id", "old_covariate_id",
                      "level", "dim_name")
    outfile <- file(covmapoutfilepath, "w")
    writeLines(header, outfile)
    close(outfile)

    # Write headers for covariates.
    header <- sprintf("%s\t%s\t%s", "new_person_id", "new_covariate_id",
                      "new_covariate_value")
    for (outfilepath in c(reqoutfilepath, optoutfilepath)) {
        outfile <- file(outfilepath, "w")
        writeLines(header, outfile)
        close(outfile)
    }

    newbasecovid <- 1
    for (infilepath in c(reqinpaths, optinpaths)) {
        dimname <- file_path_sans_ext(basename(infilepath))
        required <- (infilepath %in% reqinpaths)
        if (required) {
            outfilepath <- reqoutfilepath
        } else {
            outfilepath <- optoutfilepath
        }

        covariates <- read.table(infilepath, header = TRUE, sep = '\t',
                                 col.names = c("old_person_id",
                                 "old_covariate_id", "old_covariate_value"),
                                 colClasses = c(old_person_id="character",
                                 old_covariate_id="character",
                                 old_covariate_value="numeric"))

        covariates <- merge(pidMap, covariates)
        covariates <- covariates[, c('new_person_id', 'old_covariate_id',
                                     'old_covariate_value')]

        # Get use cutoff to drop covariates.
        covcounts <- aggregate(old_covariate_value ~ old_covariate_id,
                               covariates, length)
        covcounts <- covcounts[order(-covcounts$old_covariate_value),]
        localcutoff <- min(cutoff, length(covcounts$old_covariate_id))
        covstokeep <- covcounts[1:localcutoff,]$old_covariate_id
        covariates <- covariates[covariates$old_covariate_id %in% covstokeep,]


        # Get median and 75th percentile values.
        mids <- aggregate(old_covariate_value ~ old_covariate_id, covariates,
                          median)
        mids <- setNames(mids, c("old_covariate_id", "medians"))
        covariates <- merge(covariates, mids)
        highs <- aggregate(old_covariate_value ~ old_covariate_id, covariates,
                           seventyfifth)
        highs <- setNames(highs, c("old_covariate_id", "seventyfifth"))
        covariates <- merge(covariates, highs)

        # Set the correct level for each covariate.
        covariates$level <- rep("l", length(covariates$new_person_id))
        mask <- covariates$old_covariate_value >= covariates$medians
        covariates$level[mask] <- "m"
        mask <- covariates$old_covariate_value >= covariates$seventyfifth
        covariates$level[mask] <- "h"

        # Get new covariate ids.
        covMap <- data.frame(old_covariate_id=covariates$old_covariate_id,
                             level=covariates$level)
        covMap <- unique(covMap)
        covMap$dim_name <- rep(dimname, length(nrow(covMap)))
        finalnewcovid <- newbasecovid + nrow(covMap) - 1
        covMap$new_covariate_id <- newbasecovid:finalnewcovid
        covMap <- covMap[c("new_covariate_id", "old_covariate_id",
                           "level", "dim_name")]

        # Write out covariate map.
        write.table(covMap, file = covmapoutfilepath, append = TRUE,
                    sep = '\t', row.names = FALSE, col.names = FALSE)

        # Increase new covariate counter for next round.
        newbasecovid <- finalnewcovid + 1

        # Transform new covariates.
        covariates <- merge(covariates, covMap)
        covariates <- covariates[c('new_person_id', 'new_covariate_id')]
        covariates$new_covariate_value <- rep(1, nrow(covariates))

        # Write out covariates.
        write.table(covariates, file = outfilepath, append = TRUE,
                    sep = '\t', row.names = FALSE, col.names = FALSE)
    }
}


seventyfifth <- function(values) {
    pos <- 3*length(values)/4
    mean(c(values[floor(pos)], values[ceiling(pos)]))
}

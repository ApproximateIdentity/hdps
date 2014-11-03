#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, cutoff=NULL) {
    convertData(datadir, covariatedir, cutoff)

    extractCovariates(covariatedir)
}


EOF <- character(0)


convertData <- function(datadir, covariatedir, cutoff) {

    cat("Converting cohorts\n")
    pidMap <- convertCohorts.test(datadir, covariatedir)

    cat("Converting outcomes\n")
    convertOutcomes.test(datadir, covariatedir, pidMap)

    cat("Converting covariates\n")
    convertCovariates.test(datadir, covariatedir, pidMap, cutoff)
}


extractCovariates <- function(covariatedir) {
}


convertOutcomes.test <- function(datadir, covariatedir, pidMap) {
    infilepath <- file.path(datadir, "outcomes.csv")
    outcomes <- read.table(infilepath, header = TRUE, sep = '\t',
                           col.names = c("old_person_id", "outcome_id"),
                           colClasses = c(old_person_id="character",
                           outcome_id="numeric"))
    
    outcomes <- merge(pidMap, outcomes)[, c('new_person_id', 'outcome_id')]
    outfilepath <- file.path(covariatesdir, "outcomes.csv")
    write.table(outcomes, file = outfilepath, sep = '\t', row.names = FALSE)
}


convertOutcomes <- function(datadir, covariatedir, pidMap) {
    # Needs to be fixed!
    infilepath <- file.path(datadir, "outcomes.csv")
    infile <- file(infilepath, "r")

    outfilepath <- file.path(covariatesdir, "outcomes.csv")
    outfile <- file(outfilepath, "w")

    # Take care of headers.
    readLines(infile, n = 1)
    writeLines("new_person_id\toutcome_id", outfile)

    line <- readLines(infile, n = 1)
    while (!identical(line, EOF)) {
        row <- strsplit(line, '\t')[[1]]
        old_person_id <- row[1]
        outcome_id <- row[2]
        new_person_id <- pidMap[[old_person_id]]

        outline <- sprintf("%s\t%s", new_person_id, outcome_id)
        writeLines(outline, outfile)

        line <- readLines(infile, n = 1)
    }
    
    
    close(outfile)
    close(infile)
}


convertCohorts.test <- function(datadir, covariatedir) {
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


convertCohorts <- function(datadir, covariatedir) {
    pidMap <- list()

    filepath <- file.path(datadir, "cohorts.csv")
    infile <- file(filepath, "r")
    outfilepath <- file.path(covariatesdir, "cohorts.csv")
    outfile <- file(outfilepath, "w")

    # Skip old header.
    readLines(infile, n = 1)

    # Write out new header.
    header <- sprintf("%s\t%s", "new_person_id", "cohort_id")
    writeLines(header, outfile)

    new_person_id <- 1
    line <- readLines(infile, n = 1)
    while (!identical(line, EOF)) {
        row <- strsplit(line, '\t')[[1]]
        old_person_id <- row[1]
        cohort_id <- row[2]
        outline <- sprintf("%s\t%s", new_person_id, cohort_id)
        writeLines(outline, outfile)

        pidMap[[old_person_id]] <- toString(new_person_id)

        new_person_id <- new_person_id + 1
        line <- readLines(infile, n = 1)
    }

    close(outfile)
    close(infile)

    # Save pidMap.
    outfilepath <- file.path(covariatesdir, "personMap.csv")
    outfile <- file(outfilepath, "w")
    writeLines("new_person_id\told_person_id", outfile)
    for (i in 1:length(pidMap)) {
        oldpid <- names(pidMap)[i]
        newpid <- pidMap[[oldpid]]
        line <- sprintf("%s\t%s", newpid, oldpid)
        writeLines(line, outfile)
    }
    close(outfile)

    pidMap
}


convertCovariates.test <- function(datadir, covariatedir, pidMap, cutoff) {
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


convertCovariates <- function(datadir, covariatedir, pidMap, cutoff) {
    if (is.null(cutoff)) {
        # Infinity.
        cutoff = 1e10
    }

    covMap <- list()
    
    reqoutfilepath <- file.path(covariatesdir, "requiredcovariates.csv")
    reqoutfile <- file(reqoutfilepath, "w")
    optoutfilepath <- file.path(covariatesdir, "optionalcovariates.csv")
    optoutfile <- file(optoutfilepath, "w")

    reqdir <- file.path(datadir, "dimensions", "required")
    optdir <- file.path(datadir, "dimensions", "optional")
    reqpaths <- list.files(reqdir, full.names=TRUE)
    optpaths <- list.files(optdir, full.names=TRUE)

    # Write header.
    header <- sprintf("%s\t%s\t%s", "person_id", "covariate_id",
                      "covariate_value")
    writeLines(header, reqoutfile)
    writeLines(header, optoutfile)

    timer <- Timer$new()
    newcovid <- 1
    for (infilepath in c(reqpaths, optpaths)) {
        dimname <- file_path_sans_ext(basename(infilepath))
        cat(sprintf("Converting dimension %s\n", dimname))

        required <- (infilepath %in% reqpaths)
        if (required) {
            outfile <- reqoutfile
        } else {
            outfile <- optoutfile
        }

        infile <- file(infilepath, "r")

        # Drop header.
        #readLines(infile, n = 1)
        #lines <- readLines(infile)
        #close(infile)

        # TODO: Value should be called "covariate_value".
        data <- read.csv(infilepath, sep = "\t",
                         col.names = c("person_id", "covariate_id", "value"),
                         colClasses = c(person_id="character",
                         covariate_id="character", value="numeric"))

        oldcovids <- unique(data$covariate_id)
        oldcovvalues <- split(data$value, data$covariate_id)

        midvals <- lapply(oldcovvalues, median)
        highvals <- lapply(oldcovvalues, seventyfifth)
        numvals <- lapply(oldcovvalues, sum)

        # TODO: Vectorize this.
        for (oldcovid in oldcovids) {
            lowkey <- sprintf("%s%sl", dimname, oldcovid)
            covMap[[lowkey]] <- newcovid
            newcovid <- newcovid + 1

            midkey <- sprintf("%s%sm", dimname, oldcovid)
            covMap[[midkey]] <- newcovid
            newcovid <- newcovid + 1

            highkey <- sprintf("%s%sh", dimname, oldcovid)
            covMap[[highkey]] <- newcovid
            newcovid <- newcovid + 1
        }

        f <- function(row) {
            oldcovid <- row[[2]]
            oldcovvalue <- as.integer(row[[3]])

            if (oldcovvalue >= highvals[[oldcovid]]) {
                highkey <- sprintf("%s%sh", dimname, oldcovid)
                newcovid <- covMap[[highkey]]
            } else if (oldcovvalue >= midvals[[oldcovid]]) {
                midkey <- sprintf("%s%sm", dimname, oldcovid)
                newcovid <- covMap[[midkey]]
            } else {
                newcovid <- 1
            }

            newcovid
        }
        new_covariate_id <- apply(data, MARGIN=1, FUN=f)
            
        f <- function(oldpid) {
            pidMap[[oldpid]]
        }
        new_person_id <- vapply(data$person_id, f, "", USE.NAMES = FALSE)

        # TODO: Does not appear to work.
        # Get list of covariates to keep or cutoff.
        numvals <- numvals[order(unlist(numvals), decreasing = TRUE)]
        covstokeep <- list()
        for (i in 1:length(numvals)) {
            covid <- names(numvals)[i]
            covstokeep[[covid]] <- (i <= cutoff)
        }

        # TODO: Vectorize this.
        # Write out the covariates.
        #for (line in lines) {
            #row <- strsplit(line, '\t')[[1]]
            #oldpid <- row[[1]]
            #oldcovid <- row[[2]]
            #oldcovvalue <- as.integer(row[[3]])

            # Do not write out covariates lower than the cutoff.
            #if (!required && !covstokeep[[oldcovid]]) {
                #next
            #}

            #newpid <- pidMap[[oldpid]]

            #if (oldcovvalue >= highvals[[oldcovid]]) {
                #highkey <- sprintf("%s%sh", dimname, oldcovid)
                #newcovid <- covMap[[highkey]]
            #} else if (oldcovvalue >= midvals[[oldcovid]]) {
                #midkey <- sprintf("%s%sm", dimname, oldcovid)
                #newcovid <- covMap[[midkey]]
            #} else {
                #newcovid <- 1
            #}
            
            #newcovval <- 1

            #line <- sprintf("%s\t%s\t%s", newpid, newcovid, newcovval)
            #writeLines(line, outfile)
        #}
        write.table(data.frame(new_person_id, new_covariate_id, rep(1,
                               length(new_person_id))),
                    outfile, append = TRUE, row.names = FALSE,
                    col.names = FALSE)
        print(timer$split())
    }

    close(reqoutfile)
    close(optoutfile)

    # Save covMap.
    outfilepath <- file.path(covariatesdir, "covariateMap.csv")
    outfile <- file(outfilepath, "w")
    writeLines("new_covariate_id\told_covariate_id", outfile)
    for (i in 1:length(covMap)) {
        oldcovid <- names(covMap)[i]
        newcovid <- covMap[[oldcovid]]
        line <- sprintf("%s\t%s", newcovid, oldcovid)
        writeLines(line, outfile)
    }
    close(outfile)
}


seventyfifth <- function(values) {
    pos <- 3*length(values)/4
    mean(c(values[floor(pos)], values[ceiling(pos)]))
}

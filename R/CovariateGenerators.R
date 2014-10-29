#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, cutoff=NULL) {
    convertData(datadir, covariatedir, cutoff = NULL)

    #covariates <- subsampleCovariates(preCovariates, covariateMap)
    #saveSampleCovariates(covariatesdir, covariates)
}


EOF <- character(0)


convertData <- function(datadir, covariatedir) {
    pidMap <- convertPersons(datadir, covariatedir)
    convertCovariates(datadir, covariatedir, pidMap, cutoff)
}


convertPersons <- function(datadir, covariatedir) {
    pidMap <- list()

    filepath <- file.path(datadir, "cohorts.csv")
    infile <- file(filepath, "r")
    outfilepath <- file.path(covariatesdir, "personRecord.csv")
    outfile <- file(outfilepath, "w")

    # Skip old header.
    readLines(infile, n = 1)

    # Write out new header.
    header <- sprintf("%s\t%s\t%s\t%s", "new_person_id", "old_person_id",
                      "cohort_id", "outcome_id")
    writeLines(header, outfile)

    # TODO: Make this use actual data later.
    outcome_id <- 0

    new_person_id <- 1
    line <- readLines(infile, n = 1)
    while (!identical(line, EOF)) {
        row <- strsplit(line, '\t')[[1]]
        old_person_id <- row[1]
        cohort_id <- row[2]
        outline <- sprintf("%s\t%s\t%s\t%s", new_person_id, old_person_id,
                           cohort_id, outcome_id)
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


convertCovariates <- function(datadir, covariatedir, pidMap, cutoff) {
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
    header <- sprintf("%s\t%s\t%s\t%s", "person_id", "covariate_id",
                      "covariate_value", "required")
    writeLines(header, reqoutfile)
    writeLines(header, optoutfile)

    new_covariate_id <- 1
    for (infilepath in c(reqpaths, optpaths)) {
        dimname <- file_path_sans_ext(basename(infilepath))
        required <- (infilepath %in% reqpaths)
        if (required) {
            outfile <- reqoutfile
        } else {
            outfile <- optoutfile
        }

        infile <- file(infilepath, "r")
        # Drop header.
        readLines(infile, n = 1)
        lines <- readLines(infile)
        close(infile)

        oldcovvalues <- list()
        for (line in lines) {
            row <- strsplit(line, '\t')[[1]]
            oldpid <- row[[1]]
            oldcovid <- row[[2]]
            oldcovvalue <- as.integer(row[[3]])

            if (is.null(oldcovvalues[[oldcovid]])) {
                oldcovvalues[[oldcovid]] <- c(oldcovvalue)
            } else {
                newpos <- length(oldcovvalues[[oldcovid]]) + 1
                oldcovvalues[[oldcovid]][[newpos]] <- oldcovvalue
            }
        }

        midvals <- list()
        highvals <- list()
        for (i in 1:length(oldcovvalues)) {
            oldcovid <- names(oldcovvalues)[i]
            values <- sort(oldcovvalues[[oldcovid]])

            len <- length(values)

            # TODO: Make this use correct median and 75 percentile functions.
            midval <- values[ceiling(len/2)]
            highval <- values[ceiling(3*len/4)]

            midvals[[oldcovid]] <- midval
            highvals[[oldcovid]] <- highval

            lowkey <- sprintf("%s%sl", dimname, oldcovid)
            covMap[[lowkey]] <- new_covariate_id
            new_covariate_id <- new_covariate_id + 1

            midkey <- sprintf("%s%sm", dimname, oldcovid)
            covMap[[midkey]] <- new_covariate_id
            new_covariate_id <- new_covariate_id + 1

            highkey <- sprintf("%s%sh", dimname, oldcovid)
            covMap[[highkey]] <- new_covariate_id
            new_covariate_id <- new_covariate_id + 1
        }

        # Write out the covariates.
        for (line in lines) {
            row <- strsplit(line, '\t')[[1]]
            oldpid <- row[[1]]
            oldcovid <- row[[2]]
            oldcovvalue <- as.integer(row[[3]])

            newpid <- pidMap[[oldpid]]

            if (oldcovvalue >= highvals[[oldcovid]]) {
                highkey <- sprintf("%s%sh", dimname, oldcovid)
                newcovid <- covMap[[highkey]]
            } else if (oldcovvalue >= midvals[[oldcovid]]) {
                midkey <- sprintf("%s%sm", dimname, oldcovid)
                newcovid <- covMap[[midkey]]
            } else {
                newcovid <- 1
            }
            
            newcovval <- 1

            line <- sprintf("%s\t%s\t%s\t%s", newpid, newcovid, newcovval,
                            required)
            writeLines(line, outfile)
        }
    }

    close(reqoutfile)
    close(optoutfile)

    # Save covMap.
    outfilepath <- file.path(covariatesdir, "covariateMap.csv")
    outfile <- file(outfilepath, "w")
    writeLines("new_covariate_id\told_covariate_id", outfile)
    for (i in 1:length(covMap)) {
        oldcovid <- names(covMap)[i]
        newcovid <- pidMap[[oldcovid]]
        line <- sprintf("%s\t%s", newcovid, oldcovid)
        writeLines(line, outfile)
    }
    close(outfile)
}


getCovariateRecord <- function(datadir) {
    # HACK: The initial row is necessary to avoid problems with the integer64
    # data type which result from built-in type coersions within R.
    covariateRecord <- data.frame(
        new_covariate = 0,
        old_covariate = as.integer64.character('0'),
        level = "",
        dim = "",
        required = FALSE,
        stringsAsFactors = FALSE)

    reqdir <- file.path(datadir, "dimensions", "required")
    optdir <- file.path(datadir, "dimensions", "optional")
    reqpaths <- list.files(reqdir, full.names=TRUE)
    optpaths <- list.files(optdir, full.names=TRUE)
    for (filepath in c(reqpaths, optpaths)) {
        required <- (filepath %in% reqpaths)
        dimName <- file_path_sans_ext(basename(filepath))

        oldCov <- unique(fread(filepath)$concept_id)
        numOldCov <- length(oldCov)

        # The three levels correspond to 1, median, and 75% percentile as
        # described in the HDPS algorithm.
        level <- rep(c("l", "m", "h"), numOldCov)

        # Replicate the dimName to fit the column of the data frame.
        dimNames <- rep(dimName, numOldCov * 3)

        # Replicate the required to fit the column of the data frame.
        required <- rep(required, numOldCov * 3)
        
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
            dim = dimNames,
            required = required)

        covariateRecord <- rbind(covariateRecord, newCovariateRecord)
    }

    # Remove the initial row and reindex the data frame.
    covariateRecord <- covariateRecord[-1, ]
    rownames(covariateRecord) <- 1:nrow(covariateRecord)
    
    covariateRecord
}


saveCovariateRecord <- function(covariatesdir, covariateRecord) {
    filepath <- file.path(covariatesdir, "covariateRecord.csv")
    write.table(covariateRecord, file=filepath, sep="\t", row.names=FALSE)
}


getCovariateMap <- function(covariateRecord) {
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


extractPreCovariates <- function(datadir, idMap, covariateMap) {
    preCovariates <- data.frame()

    reqdir <- file.path(datadir, "dimensions", "required")
    optdir <- file.path(datadir, "dimensions", "optional")
    reqpaths <- list.files(reqdir, full.names=TRUE)
    optpaths <- list.files(optdir, full.names=TRUE)
    for (filepath in c(reqpaths, optpaths)) {
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
            person <- idMap[toString(person)]

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
            covariate <- covariateMap[key]

            person_id[length(person_id) + 1] <- person
            covariate_id[length(covariate_id) + 1] <- covariate
        }
        
        # The covariate value is always 1 in HDPS.
        covariate_value = rep(1, length(person_id))

        new_covariates <- data.frame(person_id, covariate_id, covariate_value)
        preCovariates <- rbind(preCovariates, new_covariates)

        msg <- sprintf("Done extracting covariates from dimension %s\n",
                       dimName)
        cat(msg)
    }

    preCovariates
}


savePreCovariates <- function(covariatesdir, preCovariates) {
    filepath <- file.path(covariatesdir, "precovariates.csv")
    write.table(preCovariates, file=filepath, sep="\t", row.names=FALSE)
}


subsampleCovariates <- function(covariates, covariateMap) {
    covariates
}

saveSampleCovariates <- function(covariatesdir, covariates) {
    filepath <- file.path(covariatesdir, "covariates.csv")
    write.table(covariates, file=filepath, sep="\t", row.names=FALSE)
}


# TODO: Replace this with "median" function. For some reason using median
# throws errors above.
fiftiethPercentile <- function(numbers) {
    quantile(numbers)[[3]]
}


seventyFifthPercentile <- function(numbers) {
    quantile(numbers)[[4]]
}

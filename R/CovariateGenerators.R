#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, cutoff=NULL) {
    convertData(datadir, covariatedir, cutoff)

    extractCovariates(covariatedir)
}


EOF <- character(0)


convertData <- function(datadir, covariatedir, cutoff) {
    pidMap <- convertPersons(datadir, covariatedir)
    convertCovariates(datadir, covariatedir, pidMap, cutoff)
}


extractCovariates <- function(covariatedir) {
}


convertPersons <- function(datadir, covariatedir) {
    pidMap <- list()

    filepath <- file.path(datadir, "cohorts.csv")
    infile <- file(filepath, "r")
    outfilepath <- file.path(covariatesdir, "persons.csv")
    outfile <- file(outfilepath, "w")

    # Skip old header.
    readLines(infile, n = 1)

    # Write out new header.
    header <- sprintf("%s\t%s\t%s", "new_person_id", "cohort_id",
                      "outcome_id")
    writeLines(header, outfile)

    # TODO: Make this use actual data later.
    outcome_id <- 0

    new_person_id <- 1
    line <- readLines(infile, n = 1)
    while (!identical(line, EOF)) {
        row <- strsplit(line, '\t')[[1]]
        old_person_id <- row[1]
        cohort_id <- row[2]
        outline <- sprintf("%s\t%s\t%s", new_person_id, cohort_id, outcome_id)
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
        numvals <- list()
        for (i in 1:length(oldcovvalues)) {
            oldcovid <- names(oldcovvalues)[i]
            values <- sort(oldcovvalues[[oldcovid]])

            len <- length(values)
            numvals[[oldcovid]] <- sum(values)

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

        # Get list of covariates to keep or cutoff.
        numvals <- numvals[order(unlist(numvals), decreasing = TRUE)]
        covstokeep <- list()
        for (i in 1:length(numvals)) {
            covid <- names(numvals)[i]
            covstokeep[[covid]] <- (i <= cutoff)
        }

        # Write out the covariates.
        for (line in lines) {
            row <- strsplit(line, '\t')[[1]]
            oldpid <- row[[1]]
            oldcovid <- row[[2]]
            oldcovvalue <- as.integer(row[[3]])

            # Do not write out covariates lower than the cutoff.
            if (!required && !covstokeep[[oldcovid]]) {
                next
            }

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

            line <- sprintf("%s\t%s\t%s", newpid, newcovid, newcovval)
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

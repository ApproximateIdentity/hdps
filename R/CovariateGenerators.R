#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, cutoff=NULL) {
    convertData(datadir, covariatedir, cutoff)

    extractCovariates(covariatedir)
}


EOF <- character(0)


convertData <- function(datadir, covariatedir, cutoff) {

    cat("Converting cohorts\n")
    pidMap <- convertCohorts(datadir, covariatedir)

    cat("Converting outcomes\n")
    convertOutcomes(datadir, covariatedir, pidMap)

    cat("Converting covariates\n")
    convertCovariates(datadir, covariatedir, pidMap, cutoff)
}


extractCovariates <- function(covariatedir) {
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

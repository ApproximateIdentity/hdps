# @file DataGenerators.R
#
# Copyright 2014 Observational Health Data Sciences and Informatics
#
# This file is part of hdps
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author Observational Health Data Sciences and Informatics
# @author Thomas Nyberg

#' @export
generateDataFromSql <- function(
    connectionDetails,
    cohortDetails,
    outcomeDetails,
    sqldir,
    datadir,
    tmpdir = NULL,
    topN = 200,
    minPatients = NULL,
    DEBUG = FALSE) {

    if (is.null(topN)) {
        # Infinity.
        topN <- 1000000000
    }

    if (is.null(minPatients)) {
        # Infinity.
        minPatients <- 0
    }

    if (is.null(tmpdir)) {
        tmpdir <- tempdir()
    } else {
        # Clean out any old files.
        unlink(tmpdir, recursive = TRUE)
        dir.create(tmpdir)
    }

    # Clean out any old data in datadir and rebuild necessary directories.
    cleanDataDir(datadir)

    # Setup sql logger.
    logfile <- file.path(tmpdir, "sql.log")
    sqllogger <- Logger$new(logfile, truncate = TRUE)

    # Check that the directories and necessary sql files exist.
    if (!validSqlStructure(sqldir)) {
        cat("Input sql invalid. Stopping analysis.\n")
        return(NULL)
    }


    if (!DEBUG) {
        # TODO: Change connectionDetails to use do.call.
        conn <- connect(
            dbms=connectionDetails$dbms,
            connectionDetails$user,
            connectionDetails$password,
            connectionDetails$server,
            connectionDetails$port,
            connectionDetails$schema)
    }


    # Build cohort sql.
    cohortsfile <- file.path(sqldir, "cohorts.sql")
    cohortsql <- readfile(cohortsfile)

    cohortparams <- c(
        list(sql = cohortsql,
             cdm_schema = connectionDetails$schema,
             results_schema = connectionDetails$schema),
        cohortDetails)

    cohortsql <- do.call(renderSql, cohortparams)$sql

    cohortsql <- translateSql(
        sql = cohortsql,
        sourceDialect = "sql server",
        targetDialect = connectionDetails$dbms)$sql

    sqllogger$log(cohortsql)

    if (!DEBUG) {
        cat("Building cohorts.\n")
        executeSql(conn, cohortsql, progressBar = FALSE,
                   reportOverallTime = FALSE)

        cat("Downloading cohorts data.\n")
        cohorts <- downloadcohorts(conn, connectionDetails$dbms)
        numpersons <- length(unique(cohorts$person_id))
        savecohorts(datadir, cohorts)
    } else {
        cat("(Debug mode) Building cohorts.\n")
        numpersons <- "NULL"
    }

    # Build outcome sql.
    outcomesfile <- file.path(sqldir, "outcomes.sql")
    outcomesql <- readfile(outcomesfile)

    outcomeparams <- c(
        list(sql = outcomesql,
             cdm_schema = connectionDetails$schema,
             results_schema = connectionDetails$schema),
        outcomeDetails)

    outcomesql <- do.call(renderSql, outcomeparams)$sql

    outcomesql <- translateSql(
        sql = outcomesql,
        sourceDialect = "sql server",
        targetDialect = connectionDetails$dbms)$sql

    sqllogger$log(cohortsql)

    if (!DEBUG) {
        cat("Building outcomes.\n")
        executeSql(conn, outcomesql, progressBar = FALSE,
                   reportOverallTime = FALSE)

        cat("Downloading outcomes data.\n")
        outcomes <- downloadOutcomes(conn, connectionDetails$dbms)
        saveoutcomes(datadir, outcomes)
    } else {
        cat("(Debug mode) Building outcomes.\n")
    }

    # Handle dimensions.
    dimdir <- file.path(sqldir, "dimensions")
    reqdir <- file.path(dimdir, "required")
    optdir <- file.path(dimdir, "optional")

    reqfiles <- list.files(reqdir, full.names=TRUE)
    reqfiles <- reqfiles[is.sql.file(reqfiles)]
    optfiles <- list.files(optdir, full.names=TRUE)
    optfiles <- optfiles[is.sql.file(optfiles)]

    presql <- readfile("pre.sql", package = "hdps")
    postsql <- readfile("post.sql", package = "hdps")

    for (dimpath in c(reqfiles, optfiles)) {
        dimname <- file_path_sans_ext(basename(dimpath))
        required <- (dimpath %in% reqfiles)
        dimsql <- readfile(dimpath)

        dimsql <- paste(presql, dimsql, postsql, sep = "\n")

        if (required) {
            thisMinPatients = 0
        } else {
            thisMinPatients = minPatients
        }

        dimsql <- renderSql(sql = dimsql,
                            cdm_schema = connectionDetails$schema,
                            topN = topN,
                            numpersons = numpersons,
                            minPatients = thisMinPatients)$sql

        dimsql <- translateSql(
            sql = dimsql,
            targetDialect = connectionDetails$dbms)$sql

        sqllogger$log(dimsql)

        if (!DEBUG) {
            msg <- sprintf("Building dimension: %s\n", dimname)
            cat(msg)
            executeSql(conn, dimsql, progressBar = FALSE,
                       reportOverallTime = FALSE)

            cat("Downloading dimension data...\n")
            dim <- downloaddimension(conn, connectionDetails$dbms)

            savedimension(datadir, dimname, dim, required)
        } else {
            msg <- sprintf("(Debug mode) Building dimension: %s\n", dimname)
            cat(msg)
        }
    }

    if (!DEBUG) {
        dummy <- dbDisconnect(conn)
    }
}


downloaddimension <- function(conn, dbms) {
    sql = "
    SELECT
        d.person_id,
        d.covariate_id,
        d.covariate_count
    FROM
        #dim d INNER JOIN #prevalent_ids p
            ON d.covariate_id = p.covariate_id
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    dim <- dbGetQuery(conn, sql)

    dim
}


generateSimulatedData <- function(
    datadir,
    numpersons = 1000,
    numreqdims = 3,
    numoptdims = 10) {

    cleanDataDir(datadir)

    # Generate person ids.
    maxpid = 1000000000
    pids <- unique(floor(runif(numpersons, min = 1, max = maxpid)))

    # Generate cohorts.
    cohortids <- sample(0:1, numpersons, replace = TRUE)
    cohorts <- data.frame(
        person_id = pids,
        cohort_id = cohortids)

    filepath <- file.path(datadir, "cohorts.csv")
    write.table(cohorts, file=filepath, sep="\t", row.names=FALSE)

    # Generate outcomes.
    outcomeids <- sample(0:1, numpersons, replace = TRUE)
    outcomes <- data.frame(
        person_id = pids,
        outcome_id = outcomeids)

    # outcomes should be sparse!
    outcomes <- outcomes[outcomes$outcome_id == 1,]

    filepath <- file.path(datadir, "outcomes.csv")
    write.table(outcomes, file=filepath, sep="\t", row.names=FALSE)

    # Generate dimensions.
    reqdir <- file.path(datadir, "dimensions", "required")
    generateSimulatedDims(reqdir, pids, numreqdims)
    optdir <- file.path(datadir, "dimensions", "optional")
    generateSimulatedDims(optdir, pids, numoptdims)
}


#' @export
generateSimulatedDims <- function(outdir, pids, numdims) {
    for (i in 1:numdims) {
        filename <- sprintf("%sdim%s.csv", basename(outdir), i)
        filepath <- file.path(outdir, filename)

        newpids <- sample(pids, .5 * length(pids))
        newpids <- rep(newpids, each = 5)
        
        # Generate covariate ids.
        maxcovid = 1000000000
        covids <- floor(runif(0.7 * length(newpids), min = 1, max = maxcovid))

        # This helps force some covariate collitions.
        covs <- sample(covids,
                       length(newpids),
                       replace = TRUE)
        dim <- data.frame(
            person_id = newpids,
            covariate_id = covs,
            covariate_value = rep(1, length(newpids)))

        dim <- aggregate(covariate_value ~ person_id + covariate_id,
                         dim, sum)

        write.table(dim, file=filepath, sep="\t", row.names=FALSE)
    }
}


validSqlStructure <- function(sqldir) {
    if (!file.exists(sqldir) || !(file.info(sqldir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", sqldir)
        cat(msg)
        return(FALSE)
    }
    if (!file.exists(datadir) || !(file.info(datadir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", datadir)
        cat(msg)
        return(FALSE)
    }
    cohortsfile <- file.path(sqldir, "cohorts.sql")
    if (!file.exists(cohortsfile)) {
        msg <- sprintf("Error: File %s does not exist.\n", cohortsfile)
        cat(msg)
        return(FALSE)
    }
    outcomesfile <- file.path(sqldir, "outcomes.sql")
    if (!file.exists(outcomesfile)) {
        msg <- sprintf("Error: File %s does not exist.\n", outcomesfile)
        cat(msg)
        return(FALSE)
    }
    dimdir <- file.path(sqldir, "dimensions")
    if (!file.exists(dimdir) || !(file.info(dimdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", dimdir)
        cat(msg)
        return(FALSE)
    }
    reqdir <- file.path(dimdir, "required")
    if (!file.exists(reqdir) || !(file.info(reqdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", reqdir)
        cat(msg)
        return(FALSE)
    }
    optdir <- file.path(dimdir, "optional")
    if (!file.exists(optdir) || !(file.info(optdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", optdir)
        cat(msg)
        return(FALSE)
    }
    reqfiles <- list.files(reqdir, full.names=TRUE)
    optfiles <- list.files(optdir, full.names=TRUE)
    if ((length(reqfiles) == 0) && (length(optfiles) == 0)) {
        msg <- sprintf("Error: No files found in either %s or %s\n",
                       reqdir, optdir)
        cat(msg)
        return(FALSE)
    }

    # All tests passed.
    TRUE
}


cleanDataDir <- function(datadir) {
    unlink(datadir, recursive = TRUE)
    dimdir <- file.path(datadir, "dimensions")
    reqdir <- file.path(dimdir, "required")
    optdir <- file.path(dimdir, "optional")
    dir.create(reqdir, recursive = TRUE)
    dir.create(optdir, recursive = TRUE)
}


downloadOutcomes <- function(conn, dbms) {
    sql <- "
    SELECT DISTINCT
        person_id,
        outcome_id
    FROM #cohort_outcome
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    cohorts <- dbGetQuery(conn, sql)

    cohorts
}


saveoutcomes <- function(datadir, outcomes) {
    filepath <- file.path(datadir, "outcomes.csv")
    write.table(outcomes, file=filepath, sep="\t", row.names=FALSE)
}


downloadcohorts <- function(conn, dbms) {
    sql <- "
    SELECT DISTINCT
        person_id,
        cohort_id
    FROM #cohort_person
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    cohorts <- dbGetQuery(conn, sql)

    cohorts
}


savecohorts <- function(datadir, cohorts) {
    filepath <- file.path(datadir, "cohorts.csv")
    write.table(cohorts, file=filepath, sep="\t", row.names=FALSE)
}


savedimension <- function(datadir, dimname, dim, required) {
    filename <- paste(dimname, ".csv", sep="")
    if (required) {
        outdir <- file.path(datadir, "dimensions", "required")
    } else {
        outdir <- file.path(datadir, "dimensions", "optional")
    }
    filepath <- file.path(outdir, filename)
    write.table(dim, file=filepath, sep="\t", row.names=FALSE)
}

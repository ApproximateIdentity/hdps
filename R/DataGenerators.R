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
    sqldir,
    datadir,
    connectionDetails,
    cohortDetails,
    outcomeDetails,
    topN = 200,
    minPatients = NULL,
    debug = FALSE) {

    if (!is.null(minPatients)) {
        cat("Warning: minPatients is not yet implemented\n")
    }

    # Clean out any old data in datadir and rebuild necessary directories.
    cleanDataDir(datadir)

    # Check that the directories and necessary sql files exist.
    if (!validSqlStructure(sqldir)) {
        cat("Input sql invalid. Stopping analysis.\n")
        return(NULL)
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

    # Helper function to filter out non-sql files.
    is.sql.file <- function(filename) {
        return(file_ext(filename) == "sql")
    }
    is.sql.file <- Vectorize(is.sql.file)

    dimdir <- file.path(sqldir, "dimensions")
    reqdir <- file.path(dimdir, "required")
    optdir <- file.path(dimdir, "optional")
    reqfiles <- list.files(reqdir, full.names=TRUE)
    reqfiles <- reqfiles[is.sql.file(reqfiles)]
    optfiles <- list.files(optdir, full.names=TRUE)
    optfiles <- optfiles[is.sql.file(optfiles)]

    dimsqls <- list()
    required <- list()
    dimfiles <- c(reqfiles, optfiles)
    for (dimpath in dimfiles) {
        dimname <- file_path_sans_ext(basename(dimpath))
        dimsql <- readfile(dimpath)
        dimsql <- renderSql(sql = dimsql,
                            cdm_schema = connectionDetails$schema)$sql
        dimsql <- translateSql(
            sql = dimsql,
            targetDialect = connectionDetails$dbms)$sql
        dimsqls[[dimname]] <- dimsql
        required[[dimname]] <- (dimpath %in% reqfiles)
    }


    if (debug) {
        cat("Debug mode: No sql statements will be executed\n")
        cat("Debug log saved in /tmp/debug.log\n")

        debuglogger <- Logger$new(filepath = "/tmp/debug.log")

        debuglogger$log(" * * * Cohort details: * * * ")
        debuglogger$log(string(cohortDetails))
        debuglogger$log(" * * * Cohort params: * * * ")
        debuglogger$log(string(cohortparams))
        debuglogger$log(" * * * Outcome details: * * * ")
        debuglogger$log(string(outcomeDetails))
        debuglogger$log(" * * * Outcome params: * * * ")
        debuglogger$log(string(outcomeparams))
        debuglogger$log(" * * * Cohort sql: * * * ")
        debuglogger$log(cohortsql)
        debuglogger$log(" * * * Outcome sql: * * * ")
        debuglogger$log(outcomesql)

        for (dimname in names(dimsql)) {
            title <- sprintf(" * * * %s sql: * * * ", dimname)
            debuglogger$log(title)
            debuglogger$log(dimsqls[[dimname]])
        }

        return(NULL)
    }


    # TODO: Change connectionDetails to use do.call.
    conn <- connect(
        dbms=connectionDetails$dbms,
        connectionDetails$user,
        connectionDetails$password,
        connectionDetails$server,
        connectionDetails$port,
        connectionDetails$schema)

    cat("Building cohorts.\n")
    executeSql(conn, cohortsql, progressBar = FALSE, reportOverallTime = FALSE)

    cat("Downloading cohorts data.\n")
    cohorts <- downloadcohorts(conn, connectionDetails$dbms)
    savecohorts(datadir, cohorts)

    cat("Building outcomes.\n")
    executeSql(conn, outcomesql, progressBar = FALSE, reportOverallTime = FALSE)

    cat("Downloading outcomes data.\n")
    outcomes <- downloadOutcomes(conn, connectionDetails$dbms)
    saveoutcomes(datadir, outcomes)

    for (i in 1:length(dimsqls)) {
        dimname <- names(dimsqls)[i]
        dimsql <- dimsqls[[dimname]]

        msg <- sprintf("Building dimension: %s\n", dimname)
        cat(msg)
        builddimension(conn, dimsql, connectionDetails$dbms)

        cat("Downloading dimension data...\n")
        dim <- downloaddimension(conn, connectionDetails$dbms, topN)
        savedimension(datadir, dimname, dim, required[[dimname]])
    }

    dummy <- dbDisconnect(conn)
}


builddimension <- function(conn, dimsql, dbms) {
    sql <- "
    CREATE TABLE #dim (
        person_id bigint,
        covariate_id varchar,
        covariate_count int)
    ;
    "
    sql <- translateSql(
        sql = sql,
        targetDialect = dbms)$sql
    executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)

    executeSql(conn, dimsql, progressBar = FALSE, reportOverallTime = FALSE)
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


addDefaults <- function(details, defaultDetails) {
    if (is.null(details)) {
        return(defaultDetails)
    }

    for (key in names(defaultDetails)) {
        value <- defaultDetails[[key]]
        if (is.null(details[[key]])) {
            details[[key]] = value
        }
    }

    details
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


downloaddimension <- function(conn, dbms, topN) {
    # TODO: Probably should just return everything immediately if topN is
    # NULL to speed up on server computations.
    if (is.null(topN)) {
        # Infinity.
        topN <- 1000000000
    }
    # Create prevalence table.
    sql = "
    CREATE TABLE #prevalence (
        covariate_id varchar,
        person_count int
    );

    INSERT INTO #prevalence
    SELECT
        covariate_id,
        COUNT(DISTINCT(person_id))
    FROM
        #dim
    GROUP BY
        covariate_id
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)


    # TODO: This should probably be called once when building the cohorts.
    # Get cohort size.
    sql = "
    SELECT COUNT(DISTINCT(person_id))
    FROM cohort_person
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    result <- dbGetQuery(conn, sql)
    numpersons = result$count

    # Get prevalent ids.
    sql = "
    CREATE TABLE #prevalent_ids (
        covariate_id varchar
    )
    ;

    INSERT INTO prevalent_ids
    SELECT
        covariate_id
    FROM prevalence
    ORDER BY @(person_count/2 - @numpersons)
    LIMIT @topN
    ;
    "
    sql <- renderSql(sql = sql, numpersons = numpersons, topN = topN)$sql
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)

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

    # Clean out temp tables.
    sql = "
    TRUNCATE TABLE #dim;
    DROP TABLE #dim;

    TRUNCATE TABLE #prevalence;
    DROP TABLE #prevalence;

    TRUNCATE TABLE #prevalent_ids;
    DROP TABLE #prevalent_ids;
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)

    dim
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

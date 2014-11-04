#' @export
generateDataFromSql <- function(sqldir, datadir, connectionDetails,
                                cohortDetails, cutoff=NULL) {
    # Check that the directories and necessary files exist.
    if (!file.exists(sqldir) || !(file.info(sqldir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", sqldir)
        cat(msg)
        return(NULL)
    }
    if (!file.exists(datadir) || !(file.info(datadir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", datadir)
        cat(msg)
        return(NULL)
    }
    cohortsfile <- file.path(sqldir, "BuildCohorts.sql")
    if (!file.exists(cohortsfile)) {
        msg <- sprintf("Error: File %s does not exist.\n", cohortsfile)
        cat(msg)
        return(NULL)
    }
    dimdir <- file.path(sqldir, "dimensions")
    if (!file.exists(dimdir) || !(file.info(dimdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", dimdir)
        cat(msg)
        return(NULL)
    }
    reqdir <- file.path(dimdir, "required")
    if (!file.exists(reqdir) || !(file.info(reqdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", reqdir)
        cat(msg)
        return(NULL)
    }
    optdir <- file.path(dimdir, "optional")
    if (!file.exists(optdir) || !(file.info(optdir)$isdir)) {
        msg <- sprintf("Error: Directory %s does not exist.\n", optdir)
        cat(msg)
        return(NULL)
    }
    reqfiles <- list.files(reqdir, full.names=TRUE)
    optfiles <- list.files(optdir, full.names=TRUE)
    if ((length(reqfiles) == 0) && (length(optfiles) == 0)) {
        msg <- sprintf("Error: No files found in either %s or %s\n",
                       reqdir, optdir)
        cat(msg)
        return(NULL)
    }

    # TODO: Move somewhere global.
    defaultCohortDetails <- list(
        washoutWindow=183,
        indicationLookbackWindow=183,
        studyStartDate="",
        studyEndDate="",
        exclusionConceptIds = c(4027133, 4032243, 4146536, 2002282, 2213572,
                                2005890, 43534760, 21601019),
        exposureTable="DRUG_ERA")

    # Fill in missing cohortDetails with defaults.
    for (i in 1:length(defaultCohortDetails)) {
        key <- names(defaultCohortDetails)[i]
        value <- defaultCohortDetails[key]
        if (is.null(cohortDetails[key][[1]])) {
            cohortDetails[key] = value
        }
    }

    # Build cohort sql.
    cohortsql <- readfile(cohortsfile)
    cohortsql <- renderSql(
        sql = cohortsql,
        cdm_schema=cohortDetails$schema,
        results_schema=cohortDetails$schema,
        target_drug_concept_id=cohortDetails$drugA,
        comparator_drug_concept_id=cohortDetails$drugB,
        indication_concept_ids=cohortDetails$indicator,
        washout_window=cohortDetails$washoutWindow,
        indication_lookback_window=cohortDetails$indicationLookbackWindow,
        study_start_date=cohortDetails$studyStartDate,
        study_end_date=cohortDetails$studyEndDate,
        exclusion_concept_ids=cohortDetails$exclusionConceptIds,
        exposure_table=cohortDetails$exposureTable)$sql
    cohortsql <- translateSql(sql = cohortsql,
                              sourceDialect = "sql server",
                              targetDialect = connectionDetails$dbms)$sql

    dimsqls <- list()
    required <- list()
    # TODO: Change so that this uses correct output dir depending on whether it
    # is required or not.
    dimfiles <- c(reqfiles, optfiles)
    for (dimpath in dimfiles) {
        dimname <- file_path_sans_ext(basename(dimpath))
        dimsql <- readfile(dimpath)
        dimsql <- translateSql(sql = dimsql,
                               targetDialect = connectionDetails$dbms)$sql
        dimsqls[dimname] <- dimsql
        required[dimname] <- (dimpath %in% reqfiles)
    }

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

    cat("Building outcomes data.\n")
    outcomessql <- ""
    buildOutcomes(conn, outcomesql)

    cat("Downloading outcomes data.\n")
    outcomes <- downloadOutcomes(conn, connectionDetails$dbms)
    saveOutcomes(datadir, outcomes)

    for (i in 1:length(dimsqls)) {
        dimname <- names(dimsqls)[i]
        dimsql <- dimsqls[[dimname]]

        msg <- sprintf("Building dimension: %s\n", dimname)
        cat(msg)
        executeSql(conn, dimsql, progressBar = FALSE,
                   reportOverallTime = FALSE)

        cat("Downloading dimension data...\n")
        dim <- downloaddimension(conn, connectionDetails$dbms, cutoff)
        savedimension(datadir, dimname, dim, required[[dimname]])
    }

    dummy <- dbDisconnect(conn)
}


buildOutcomes <- function(conn, outcomesql) {
    cat("Warning: buildOutcomes not yet implemented\n")
}

downloadOutcomes <- function(conn, dbms) {
    cat("Warning: downloadOutcomes not implemented\n")
    outcomes <- NULL

    outcomes
}


saveOutcomes <- function(datadir, outcomes) {
    cat("Warning: saveOutcomes not implemented\n")

    # Needs to be fixed!
    infilepath <- file.path(datadir, "cohorts.csv")
    outcomes <- read.table(infilepath, header = TRUE, sep = '\t',
                           col.names = c("person_id", "outcome_id"),
                           colClasses = c(person_id="character",
                           outcome_id="numeric"))

    outfilepath <- file.path(datadir, "outcomes.csv")
    write.table(outcomes, file = outfilepath, sep = '\t', row.names = FALSE)
}


downloadcohorts <- function(conn, dbms) {
    sql <- "
    SELECT
        person_id,
        cohort_id
    FROM #cohort_person
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    cohorts <- dbGetQuery(conn, sql)
    cohorts
}


downloaddimension <- function(conn, dbms, cutoff) {
    # TODO: Probably should just return everything immediately if cutoff is
    # NULL.
    if (is.null(cutoff)) {
        # Infinity.
        cutoff <- 1000000000
    }
    # Create prevalence table.
    sql = "
    CREATE TABLE #prevalence (
        covariate_id bigint,
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
    # TODO: These statements should probably be combined.
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
        covariate_id bigint
    )
    ;

    INSERT INTO prevalent_ids
    SELECT
        covariate_id
    FROM prevalence
    ORDER BY @(person_count/2 - %s)
    LIMIT %s
    ;
    "
    # TODO: This should use renderSql.
    sql = sprintf(sql, numpersons, cutoff)
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

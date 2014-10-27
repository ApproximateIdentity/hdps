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

    for (i in 1:length(dimsqls)) {
        dimname <- names(dimsqls)[i]
        dimsql <- dimsqls[[dimname]]
        isRequired <- required[[dimname]]

        msg <- sprintf("Building dimension: %s\n", dimname)
        cat(msg)
        executeSql(conn, dimsql, progressBar = FALSE,
                   reportOverallTime = FALSE)

        cat("Downloading dimension data...\n")
        dim <- downloaddimension(conn, connectionDetails$dbms, cutoff)
        savedimension(datadir, dimname, dim, isRequired)
    }

    dummy <- dbDisconnect(conn)
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
        concept_id bigint,
        count int
    );

    INSERT INTO #prevalence
    SELECT
        concept_id,
        COUNT(DISTINCT(person_id))
    FROM
        #dim
    GROUP BY
        concept_id
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)$sql
    executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)


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
        concept_id bigint
    )
    ;

    INSERT INTO prevalent_ids
    SELECT
        concept_id
    FROM prevalence
    ORDER BY @(count/2 - %s)
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
        d.concept_id,
        d.count
    FROM
        #dim d INNER JOIN #prevalent_ids p
            ON d.concept_id = p.concept_id
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


savedimension <- function(datadir, dimname, dim, isRequired) {
    filename <- paste(dimname, ".csv", sep="")
    if (isRequired) {
        outdir <- file.path(datadir, "dimensions", "required")
    } else {
        outdir <- file.path(datadir, "dimensions", "optional")
    }
    filepath <- file.path(outdir, filename)
    write.table(dim, file=filepath, sep="\t", row.names=FALSE)
}

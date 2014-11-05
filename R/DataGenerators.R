# Default cohort details.
rifaximin <- 1735947
Lactulose <- 987245
MyocardialInfarction <- 35205189

# TODO: Give these numbers names.
defaultCohortDetails <- list(
    target_drug_concept_id = rifaximin,
    comparator_drug_concept_id = Lactulose,
    indication_concept_ids = MyocardialInfarction,
    washout_window = 183,
    indication_lookback_window = 183,
    study_start_date = "",
    study_end_date = "",
    exclusion_concept_ids = c(
        4027133,
        4032243,
        4146536,
        2002282,
        2213572,
        2005890,
        43534760,
        21601019),
    exposure_table = "DRUG_ERA")

# Default outcome details.
lowBackPain = 194133

defaultOutcomeDetails <- list(
    outcome_concept_ids = lowBackPain,
    # Table containing outcome information. Either 'CONDITION_OCCURRENCE' or
    # 'COHORT'.
    outcome_table = 'CONDITION_OCCURRENCE',
    # Condition type only applies if outcome_table is 'CONDITION_OCCURRENCE'.
    outcome_condition_type_concept_ids = c(
        38000215,
        38000216,
        38000217,
        38000218,
        38000183,
        38000232))

#' @export
generateDataFromSql <- function(
    sqldir,
    datadir,
    connectionDetails,
    cohortDetails = defaultCohortDetails,
    outcomeDetails = defaultOutcomeDetails,
    cutoff = NULL) {

    # Check that the directories and necessary files exist.
    if (!validSqlStructure(sqldir)) {
        cat("Input sql invalid. Stopping analysis.\n")
        return(NULL)
    }


    # Fill in missing details with defaults.
    cohortDetails <- addDefaults(cohortDetails, defaultCohortDetails)
    outcomeDetails <- addDefaults(outcomeDetails, defaultOutcomeDetails)

    # Build cohort sql.
    cohortsfile <- file.path(sqldir, "cohorts.sql")
    cohortsql <- readfile(cohortsfile)

    params <- c(
        list(sql = cohortsql,
             cdm_schema = connectionDetails$schema,
             results_schema = connectionDetails$schema),
        cohortDetails)

    cohortsql <- do.call(renderSql, params)$sql

    cohortsql <- translateSql(
        sql = cohortsql,
        sourceDialect = "sql server",
        targetDialect = connectionDetails$dbms)$sql


    # Build outcome sql.
    outcomesfile <- file.path(sqldir, "outcomes.sql")
    outcomesql <- readfile(outcomesfile)

    params <- c(
        list(sql = outcomesql,
             cdm_schema = connectionDetails$schema,
             results_schema = connectionDetails$schema),
        outcomeDetails)

    outcomesql <- do.call(renderSql, params)$sql

    outcomesql <- translateSql(
        sql = outcomesql,
        sourceDialect = "sql server",
        targetDialect = connectionDetails$dbms)$sql

    dimdir <- file.path(sqldir, "dimensions")
    reqdir <- file.path(dimdir, "required")
    optdir <- file.path(dimdir, "optional")
    reqfiles <- list.files(reqdir, full.names=TRUE)
    optfiles <- list.files(optdir, full.names=TRUE)

    dimsqls <- list()
    required <- list()
    dimfiles <- c(reqfiles, optfiles)
    for (dimpath in dimfiles) {
        dimname <- file_path_sans_ext(basename(dimpath))
        dimsql <- readfile(dimpath)
        dimsql <- translateSql(
            sql = dimsql,
            targetDialect = connectionDetails$dbms)$sql
        dimsqls[[dimname]] <- dimsql
        required[[dimname]] <- (dimpath %in% reqfiles)
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
        executeSql(conn, dimsql, progressBar = FALSE,
                   reportOverallTime = FALSE)

        cat("Downloading dimension data...\n")
        dim <- downloaddimension(conn, connectionDetails$dbms, cutoff)
        savedimension(datadir, dimname, dim, required[[dimname]])
    }

    dummy <- dbDisconnect(conn)
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


buildOutcomes <- function(conn, outcomesql) {
    cat("Warning: buildOutcomes not yet implemented\n")
}


downloadOutcomes <- function(conn, dbms) {
    sql <- "
    SELECT
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


savecohorts <- function(datadir, cohorts) {
    filepath <- file.path(datadir, "cohorts.csv")
    write.table(cohorts, file=filepath, sep="\t", row.names=FALSE)
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

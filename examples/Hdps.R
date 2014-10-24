library(hdps)

# Base repository folder. You may want to change this.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")

# Build temporary directories if necessary.
dir.create(datadimdir, showWarnings = FALSE, recursive = TRUE)
dir.create(covariatesdir, showWarnings = FALSE, recursive = TRUE)

# Login info.
connectionDetails <- list(
    password = Sys.getenv("MYPGPASSWORD"),
    dbms = "redshift",
    user = Sys.getenv("USER"),
    server = "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven",
    schema = "mslr_cdm4",
    port = "5439")

rifaximin <- 1735947
Lactulose <- 987245
MyocardialInfarction <- 35205189

# Cohort details.
cohortDetails <- list(
    drugA = rifaximin,
    drugB = Lactulose,
    indicator = MyocardialInfarction,
    schema = "mslr_cdm4")

generateDataFromSql(sqldir, datadir, connectionDetails, cohortDetails)

generateDataFromSql <- function(sqldir, datadir, connectionDetails,
                                cohortDetails, cutoff=NULL) {
    # Check that the directories and necessary files exist.
    if (!file.exists(sqldir) || !(file.info(sqldir)$isdir)) {
        print(paste("Error: Directory", sqldir, "does not exist."))
        return(NULL)
    }
    if (!file.exists(datadir) || !(file.info(datadir)$isdir)) {
        print(paste("Error: Directory", datadir, "does not exist."))
        return(NULL)
    }
    cohortsfile <- file.path(sqldir, "BuildCohorts.sql")
    if (!file.exists(cohortsfile)) {
        print(paste("Error: File", cohortsfile, "does not exist."))
        return(NULL)
    }
    dimdir <- file.path(sqldir, "dimensions")
    if (!file.exists(dimdir) || !(file.info(dimdir)$isdir)) {
        print(paste("Error: Directory", dimdir, "does not exist."))
        return(NULL)
    }
    dimfiles <- list.files(dimdir)
    if (length(dimfiles) == 0) {
        print(paste("Error: No files found in", dimdir))
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
        cdm_schema=cohortDetails$cdmSchema,
        results_schema=cohortDetails$cdmSchema,
        target_drug_concept_id=cohortDetails$drugA,
        comparator_drug_concept_id=cohortDetails$drugB,
        indication_concept_ids=cohortDetails$indicator,
        washout_window=cohortDetails$washoutWindow,
        indication_lookback_window=cohortDetails$indicationLookbackWindow,
        study_start_date=cohortDetails$studyStartDate,
        study_end_date=cohortDetails$studyEndDate,
        exclusion_concept_ids=cohortDetails$exclusionConceptIds,
        exposure_table=cohortDetails$exposureTable)
    cohortsql <- translateSql(sql = cohortsql,
                              targetDialect = connectionDetails$dbms)

    dimsqls <- list()
    for (dimpath in dimfiles) {
        dimname <- file_path_sans_ext(basename(dimpath))
        dimsql <- readfile(dimpath)
        dimsql <- translateSql(sql = dimsql,
                               targetDialect = connectionDetails$dbms)
        dimsqls[dimname] <- dimsql
    }

    conn <- connect(
        dbms=connectionDetails$dbms,
        connectionDetails$user,
        connectionDetails$password,
        connectionDetails$server,
        connectionDetails$port,
        connectionDetails$schema)

    print("Building cohorts.")
    executeSql(conn, cohortsql)

    print("Downloading cohorts data.")
    cohorts <- downloadcohorts(conn)
    savecohorts(datadir, cohorts)

    for (i in 1:length(dimsqls)) {
        dimname <- names(dimsqls)[i]
        dimsql <- dimsqls[dimname]

        print(paste("Building dimension:", dimname))
        executeSql(conn, dimsql)

        print("Downloading dimension data...")
        dim <- downloaddimension(conn, connectionDetails$dbms, cutoff)
        savedimension(datadir, dimname, dim)
    }

    close(conn)
}


savedimension <- function(datadir, dimname, dim) {
    filename <- paste(dimname, ".csv", sep="")
    filepath <- file.path(datadir, "dimensions", filename)
    write.table(dim, file=filepath, sep="\t", row.names=FALSE)
}


downloaddimension <- function(conn, dbms, cutoff) {
    if is.null(cutoff) {
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
    sql <- translateSql(sql = sql, targetDialect = dbms)
    executeSql(sql)

    # Get cohort size.
    sql = "
    SELECT COUNT(DISTINCT(person_id))
    FROM cohort_person
    ;
    "
    sql <- translateSql(sql = sql, targetDialect = dbms)
    result <- executeSql(sql)
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
    sql = sprintf(query, numpersons, cutoff)
    sql <- translateSql(sql = sql, targetDialect = dbms)
    executeSql(sql)

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
    sql <- translateSql(sql = sql, targetDialect = dbms)
    dim <- executeSql(sql)

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
    sql <- translateSql(sql = sql, targetDialect = dbms)
    executeSql(sql)

    return(dim)
}



savecohorts(datadir, cohorts) {
    filepath <- file.path(datadir, "cohorts.csv")
    write.table(cohorts, file=filepath, sep="\t", row.names=FALSE)
}

downloadcohorts <- function(conn) {
    sql <- "
    SELECT
        person_id,
        cohort_id
    FROM #cohort_person
    ;
    "
    cohorts <- executeSql(conn, sql)
    cohorts
}


readfile <- function(filepath) {
    text <- readChar(filepath, file.info(filepath)$size)
    return(text)
}


generateDataFromSql(sqldir, datadir, connectionDetails, cohortDetails)


# Condition to check for.

hdps = Hdps$new()
#hdps$toggledebug()

# Set to true to download all the data.
if (FALSE) {
hdps$connect(dbms, user, password, server, port, schema)

hdps$buildCohorts(rifaximin, Lactulose, MyocardialInfarction)
#hdps$getCohortSize()
query <- "
SELECT
    person_id,
    cohort_id
FROM #cohort_person
;
"
cohortdata <- hdps$connection$executeforresult(query)

filepath <- file.path(datadir, "cohorts.csv")
write.table(cohortdata, file=filepath, sep="\t", row.names=FALSE)


# Generate and download dimensions data.
filepaths <- list.files(sqldimdir, full.name=TRUE)
for (filepath in filepaths) {
    dimensionname <- file_path_sans_ext(basename(filepath))

    parametrizedSql <- loadLocalSql(filepath)
    hdps$buildDimension(parametrizedSql)

    dimensiondata <- hdps$extractDimensionData()
    filename <- paste(dimensionname, ".csv", sep="")
    filepath <- file.path(datadimdir, filename)
    write.table(dimensiondata, file=filepath, sep="\t", row.names=FALSE)
    }

hdps$disconnect()

}

# Build name record.
filepath <- file.path(datadir, "cohorts.csv")
cohorts <- fread(filepath)
nameRecord <- data.frame(new_id = 1:length(cohorts$person_id),
                         old_id = cohorts$person_id)
cohorts$person_id <- nameRecord$new_id

filepath <- file.path(covariatesdir, "nameRecord.csv")
write.table(nameRecord, filepath, sep="\t", row.names=FALSE)

filepath <- file.path(covariatesdir, "cohorts.csv")
write.table(cohorts, filepath, sep="\t", row.names=FALSE)

# Build covariate record.
filepaths <- list.files(datadimdir, full.names=TRUE)
covariateRecord <- buildCovariateRecord(filepaths)
filepath <- file.path(covariatesdir, "covariateRecord.csv")
write.table(covariateRecord, file=filepath, sep="\t", row.names=FALSE)

# Build covariate and person mappings.
covariateMapping <- buildCovariateMapping(covariateRecord)
nameMapping <- nameRecord$new_id
names(nameMapping) <- nameRecord$old_id

# Next build covariates locally.
filepaths <- list.files(datadimdir, full.names=TRUE)
covariates <- extractCovariates(filepaths, nameMapping, covariateMapping)
filepath <- file.path(covariatesdir, "covariates.csv")
write.table(covariates, file=filepath, sep="\t", row.names=FALSE)


library(Cyclops)

# Load data.
filepath <- file.path(covariatesdir, "covariates.csv")
covariates <- read.table(filepath, header=TRUE)
filepath <- file.path(covariatesdir, "cohorts.csv")
cohorts <- read.table(filepath, header=TRUE)

numPersons <- max(cohorts$person_id)
numCovariates <- max(covariates$covariate_id)

X <- Matrix(0, nrow = numPersons, ncol = numCovariates, sparse = TRUE)
for (r in 1:nrow(covariates)) {
    row <- covariates[r,]
    i <- row$person_id
    j <- row$covariate_id
    val <- row$covariate_value
    X[i, j] <- val
}

y <- arrange(cohorts, person_id)$cohort_id

cyclopsData <- createCyclopsDataFrame(y = y, sx = X, modelType = "pr")
cyclopsFit <- fitCyclopsModel(cyclopsData, prior = prior("laplace"))
#summary(cyclopsFit)

pred <- predict(cyclopsFit)

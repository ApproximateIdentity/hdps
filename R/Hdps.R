Hdps.connect <- function(
    dbms,
    user,
    password,
    server,
    port,
    schema) {

    connection$connect(
        dbms,
        user,
        password,
        server,
        port,
        schema)

    connection$schema <<- schema
}


Hdps.disconnect <- function() {
    connection$disconnect()
}


Hdps.buildCohorts <- function(
    drugA,
    drugB,
    indicator,
    washoutWindow=183,
    indicationLookbackWindow=183,
    studyStartDate="",
    studyEndDate="",
    exclusionConceptIds = c(4027133, 4032243, 4146536,
        2002282, 2213572, 2005890, 43534760, 21601019),
    exposureTable="DRUG_ERA") {

    parametrizedSql = loadSql("BuildCohorts.sql")

    cdmSchema = connection$schema
    renderedSql <- renderSql(
        sql = parametrizedSql,
        cdm_schema=cdmSchema,
        results_schema=cdmSchema,
        target_drug_concept_id=drugA,
        comparator_drug_concept_id=drugB,
        indication_concept_ids=indicator,
        washout_window=washoutWindow,
        indication_lookback_window=indicationLookbackWindow,
        study_start_date=studyStartDate,
        study_end_date=studyEndDate,
        exclusion_concept_ids=exclusionConceptIds,
        exposure_table=exposureTable)
    connection$execute(renderedSql$sql)
}


Hdps.buildDimension <- function(parametrizedSql, ...) {
    renderedSql <- renderSql(sql = parametrizedSql, ...)
    connection$execute(renderedSql$sql)
}


Hdps.extractDimensionData <- function(cutoff = 100) {
    # Create prevalence table.
    query = "
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
    connection$execute(query)

    # TODO: This should probably be an attribute of the class.
    # Get cohort size.
    query = "
    SELECT COUNT(DISTINCT(person_id))
    FROM cohort_person
    ;
    "
    result = connection$executeforresult(query)
    numpersons = result$count

    # Get prevalent ids.
    query = "
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
    query = sprintf(query, numpersons, cutoff)
    connection$execute(query)

    query = "
    SELECT
        d.person_id,
        d.concept_id,
        d.count
    FROM
        #dim d INNER JOIN #prevalent_ids p
            ON d.concept_id = p.concept_id
    ;
    "
    dimensiondata = connection$executeforresult(query)

    # Clean out temp tables.
    query = "
    TRUNCATE TABLE #dim;
    DROP TABLE #dim;

    TRUNCATE TABLE #prevalence;
    DROP TABLE #prevalence;

    TRUNCATE TABLE #prevalent_ids;
    DROP TABLE #prevalent_ids;
    ;
    "
    connection$execute(query)

    return(dimensiondata)
}


# TODO: Move this somewhere more appropriate. Probably to a section on data
# conversion.
# TODO: A cutoff should be added back in here. This will be redundant with how
# the data is downloaded and produced (which should be the case).



Hdps.getCohortSize <- function() {
    # This sql has no parameters, though it should probably have some so that
    # it can be used for any table.
    renderedSql <- loadSql("GetCohortSize.sql")
    connection$executeforresult(renderedSql)
}


Hdps.toggledebug <- function() {
    if (debug) {
        connection$debug <<- FALSE
        debug <<- FALSE
        print("debug mode off")
    } else {
        connection$debug <<- TRUE
        debug <<- TRUE
        print("debug mode on")
    }
}


#' @export
Hdps <- setRefClass(
    "Hdps",
    fields=list(
        connection = "ANY",
        debug = "ANY"
    ),
    methods=list(
        initialize = function(...) {
            callSuper(...)
            connection <<- Connection$new()
            debug <<- FALSE
        },
        connect = Hdps.connect,
        disconnect = Hdps.disconnect,
        buildCohorts = Hdps.buildCohorts,
        getCohortSize = Hdps.getCohortSize,
        buildDimension = Hdps.buildDimension,
        toggledebug = Hdps.toggledebug,
        extractDimensionData = Hdps.extractDimensionData
    )
)

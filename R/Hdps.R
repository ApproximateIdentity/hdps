Hdps.connect <- function(
    dbms,
    user,
    password,
    server,
    port,
    schema) {

    connection <<- Connection$new(
        password=password,
        dbms=dbms,
        user=user,
        server=server,
        schema=schema,
        port=port)
    connection$connect()
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


Hdps.getCohortSize <- function() {
    # This sql has no parameters, though it should probably have some so that
    # it can be used for any table.
    renderedSql = loadSql("GetCohortSize.sql")
    connection$executeforresult(renderedSql)
}


#' @export
Hdps <- setRefClass(
    "Hdps",
    fields=list(
        connection = "ANY"
    ),
    methods=list(
        initialize = function(...) {
          callSuper(...)
        },
        connect = Hdps.connect,
        disconnect = Hdps.disconnect,
        buildCohorts = Hdps.buildCohorts,
        getCohortSize = Hdps.getCohortSize
    )
)

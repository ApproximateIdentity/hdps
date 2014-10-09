library(hdps)
library(tutils)

logger <- Logger$new()

# Login info.
password <- Sys.getenv("MYPGPASSWORD")
dbms <- "redshift"
user <- Sys.getenv("USER")
server <- "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven"
schema <- "mslr_cdm4"
port <- "5439"

parametrizedSql <- loadSql("Test.sql")
renderedSql <- renderSql(parametrizedSql, results_schema = 'CCAE_CDM4')$sql
logger$log(renderedSql)

connection <- Connection$new(
    dbms=dbms,
    user=user,
    password=password,
    server=server,
    port=port,
    schema=schema)    

connection$connect()
connection$execute(renderedSql)

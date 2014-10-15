library(hdps)

# Login info.
password <- Sys.getenv("MYPGPASSWORD")
dbms <- "redshift"
user <- Sys.getenv("USER")
server <- "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven"
schema <- "mslr_cdm4"
port <- "5439"

parametrizedSql <- loadSql("Test.sql")
renderedSql <- renderSql(parametrizedSql, results_schema = 'CCAE_CDM4')$sql

connection <- Connection$new()

connection$connect(dbms, user, password, server, port, schema)
connection$execute(renderedSql)

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

# Drugs to compare.
Erythromycin = 1746940
Amoxicillin = 1713332

# Condition to check for.
MyocardialInfarction = 35205189

hdps = Hdps$new()
hdps$connect(dbms, user, password, server, port, schema)

hdps$buildCohorts(Erythromycin, Amoxicillin, MyocardialInfarction)

hdps$getCohortSize()

hdps$disconnect()

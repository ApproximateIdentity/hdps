library(hdps)

# Base repository folder.
basedir <- getwd()


# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
sqldimdir <- file.path(sqldir, "dimensions")
datadir <- file.path(basedir, "data")
datadimdir <- file.path(datadir, "dimensions")
covariatesdir <- file.path(basedir, "covariates")

# Build temporary directories if necessary.
dir.create(datadimdir, showWarnings = FALSE, recursive = TRUE)
dir.create(covariatesdir, showWarnings = FALSE, recursive = TRUE)

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

rifaximin = 1735947
Lactulose = 987245

# Condition to check for.
MyocardialInfarction = 35205189

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

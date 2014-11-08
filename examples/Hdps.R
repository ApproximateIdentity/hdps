library(hdps)
library(Cyclops)

# Base repository folder. You may want to change this.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")

# Login info.
connectionDetails <- list(
    password = Sys.getenv("MYPGPASSWORD"),
    dbms = "redshift",
    user = Sys.getenv("USER"),
    server = "omop-datasets.cqlmv7nlakap.us-east-1.redshift.amazonaws.com/truven",
    schema = "mslr_cdm4",
    port = "5439")

# Cohort details.
rifaximin <- 1735947
Lactulose <- 987245
MyocardialInfarction <- 35205189

cohortDetails <- list(
    drugA = rifaximin,
    drugB = Lactulose,
    indicator = MyocardialInfarction)

#generateDataFromSql(
#    sqldir,
#    datadir,
#    connectionDetails,
#    cohortDetails = cohortDetails,
#    cutoff = 100)

cat("Generating data...\n")
generateSimulatedData(datadir)

cat("Converting covariates...\n")
generateCovariatesFromData(datadir, covariatesdir, cutoff = 100)

# Run cyclops.
cat("Running Cyclops...\n")
sparseData <- getSparseData(covariatesdir)
cyclopsData <- createCyclopsDataFrame(y = sparseData$y,
                                      sx = sparseData$X,
                                      modelType = "pr")
cyclopsFit <- fitCyclopsModel(cyclopsData, prior = prior("laplace"))
summary(cyclopsFit)
pred <- predict(cyclopsFit)

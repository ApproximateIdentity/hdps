library(hdps)
library(Cyclops)

# Base repository folder. You may want to change this.
basedir <- getwd()

# Directory containing all input sql.
sqldir <- file.path(basedir, "sql")
datadir <- file.path(basedir, "data")
covariatesdir <- file.path(basedir, "covariates")

# Build output directories if necessary.
dir.create(file.path(datadir, "dimensions", "required"),
           showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(datadir, "dimensions", "optional"),
           showWarnings = FALSE, recursive = TRUE)
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

generateDataFromSql(sqldir, datadir, connectionDetails, cohortDetails,
                    cutoff=100)
generateCovariatesFromData(datadir, covariatesdir)

# Run cyclops.
sparseData <- getSparseData(covariatesdir)
cyclopsData <- createCyclopsDataFrame(y = sparseData$y,
                                      sx = sparseData$X,
                                      modelType = "pr")
cyclopsFit <- fitCyclopsModel(cyclopsData, prior = prior("laplace"))
summary(cyclopsFit)
pred <- predict(cyclopsFit)

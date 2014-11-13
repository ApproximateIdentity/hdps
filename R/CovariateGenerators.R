# @file CovariateGenerators.R
#
# Copyright 2014 Observational Health Data Sciences and Informatics
#
# This file is part of hdps
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @author Observational Health Data Sciences and Informatics
# @author Thomas Nyberg

#' @export
generateCovariatesFromData <- function(datadir, covariatesdir, topN=NULL) {

    unlink(covariatesdir, recursive = TRUE)
    dir.create(covariatesdir)

    convertData(datadir, covariatesdir, topN)
    priority <- prioritizeOptCovariates(covariatesdir)
    addPrioritizedCovariates(covariatesdir, priority)
}

addPrioritizedCovariates <- function(covariatesdir, priority,
                                     priorityCutoff=200) {

    infilepath <- file.path(covariatesdir, "optionalcovariates.csv")
    optionalcovariates <- read.table(infilepath, header = TRUE, sep = '\t',
                                     col.names = c("new_person_id",
                                     "new_covariate_id", "new_covariate_value"),
                                     colClasses = c(new_person_id="numeric",
                                     new_covariate_id="numeric",
                                     new_covariate_value="numeric"))

    priority <- priority[order(-priority$priority),]
    idstokeep <- priority[1:priorityCutoff, "new_covariate_id"]
    
    mask <- optionalcovariates$new_covariate_id %in% idstokeep
    optionalcovariates <- optionalcovariates[mask,]

    outfilepath <- file.path(covariatesdir, "covariates.csv")
    write.table(optionalcovariates, file = outfilepath, append = TRUE,
                sep = '\t', row.names = FALSE, col.names = FALSE)
}


convertData <- function(datadir, covariatesdir, topN) {
    pidMap <- convertCohorts(datadir, covariatesdir)
    convertOutcomes(datadir, covariatesdir, pidMap)
    convertCovariates(datadir, covariatesdir, pidMap, topN)
}


prioritizeOptCovariates <- function(covariatesdir) {
    # TODO: Check the logic in this function multiple times!
    infilepath <- file.path(covariatesdir, "outcomes.csv")
    outcomes <- read.table(infilepath, header = TRUE, sep = '\t',
                           col.names = c("new_person_id", "outcome_id"),
                           colClasses = c(new_person_id="numeric",
                           outcome_id="numeric"))

    infilepath <- file.path(covariatesdir, "optionalcovariates.csv")
    # Do not need the new_covariate_value column.
    covariates <- read.table(infilepath, header = TRUE, sep = '\t',
                             colClasses = c(new_person_id="numeric",
                             new_covariate_id="numeric", "NULL"))

    # Build up priority step-by-step.
    numcovariates <- length(unique(covariates$new_covariate_id))
    priority <- data.frame(new_covariate_id = unique(covariates$new_covariate_id),
                           RR = rep(1, numcovariates))

    # Step 1: For each covariate find number of persons with that covariate.
    numbers <- aggregate(new_person_id ~ new_covariate_id, covariates,
                         length)
    numbers <- setNames(numbers, c("new_covariate_id", "numbers"))

    priority <- merge(priority, numbers)
    priority$RR <- priority$RR / priority$numbers

    # Step 2: For each covariate find number of persons without that covariate.
    numpeople <- nrow(outcomes)
    priority$RR <- priority$RR * (numpeople - priority$numbers)

    # Drop temporary column.
    priority <- priority[,c("new_covariate_id", "RR")]

    # Step 3: For each covariate find number of persons with that covariate and
    # outcome equal to 1.
    personswithoutcome <- outcomes[outcomes$outcome_id == 1,]
    personswithoutcome <- data.frame(
        new_person_id = personswithoutcome$new_person_id)

    # Restrict covariates to persons with outcome = 1.
    rescovariates <- merge(covariates, personswithoutcome)
    
    numbers <- aggregate(new_person_id ~ new_covariate_id, rescovariates,
                         length)
    numbers <- setNames(numbers, c("new_covariate_id", "numbers"))

    # Set any covariates which were dropped to zero.
    priority <- merge(priority, numbers, all = TRUE)
    priority$numbers[is.na(priority$numbers)] <- 0

    priority$RR <- priority$RR * priority$numbers

    # Step 4: For each covariate find number of persons without that covariate
    # and outcome equal to 1.
    numpeopleoutcome <- sum(outcomes$outcome_id)
    priority$RR <- priority$RR / (numpeopleoutcome - priority$numbers)

    # Drop temporary column.
    priority <- priority[,c("new_covariate_id", "RR")]

    # Next continue to compute the Bias.
    # If RR is less than 1, invert it.
    priority$RR <- vapply(
        priority$RR,
        function(x) {
            if (x < 1) {
                return(1/x)
            }
            return(x)
        },
        1)

    # Compute number of people within each cohort with the different
    # covariates.
    infilepath <- file.path(covariatesdir, "cohorts.csv")
    cohorts <- read.table(infilepath, header = TRUE, sep = '\t',
                          col.names = c("new_person_id", "outcome_id"),
                          colClasses = c(new_person_id="numeric",
                          outcome_id="numeric"))

    covariates <- merge(covariates, cohorts)
    mask <- covariates$outcome_id == 1
    covariates <- covariates[mask,]
    PC1 <- aggregate(new_person_id ~ new_covariate_id,
                     covariates,
                     length)
    PC1 <- setNames(PC1, c("new_covariate_id", "PC1"))

    priority <- merge(priority, PC1, all = TRUE)
    priority$PC1[is.na(priority$PC1)] <- 0

    priority$PC0 <- numpeople - priority$PC1

    # Compute the prioity.
    priority$priority <- ((priority$PC1 * (priority$RR - 1) + 1) /
                          (priority$PC0 * (priority$RR - 1) + 1) )

    # If the priority is NaN, that means that RR was infinity. If RR is
    # infinity, we want priority to simply be PC1/PC0. (Usually this means that
    # PC1 is equal to 0.)
    mask <- is.nan(priority$priority)
    priority[mask,]$priority <- priority[mask,]$PC1 / priority[mask,]$PC0

    # Next we scale the priority.
    priority$priority <- abs(log(priority$priority))

    priority <- priority[, c("new_covariate_id", "priority")]
    priority
}


convertOutcomes <- function(datadir, covariatesdir, pidMap) {
    infilepath <- file.path(datadir, "outcomes.csv")
    outcomes <- read.table(infilepath, header = TRUE, sep = '\t',
                           col.names = c("old_person_id", "outcome_id"),
                           colClasses = c(old_person_id="character",
                           outcome_id="numeric"))

    outcomes <- merge(pidMap, outcomes)[, c('new_person_id', 'outcome_id')]
    outfilepath <- file.path(covariatesdir, "outcomes.csv")
    write.table(outcomes, file = outfilepath, sep = '\t', row.names = FALSE)
}


convertCohorts <- function(datadir, covariatesdir) {
    infilepath <- file.path(datadir, "cohorts.csv")
    cohorts <- read.table(infilepath, header = TRUE, sep = '\t',
                          col.names = c("old_person_id", "cohort_id"),
                          colClasses = c(old_person_id="character",
                          cohort_id="numeric"))

    cohorts$new_person_id <- 1:length(cohorts$old_person_id)

    pidMap <- cohorts[, c('new_person_id', 'old_person_id')]
    outfilepath <- file.path(covariatesdir, "personMap.csv")
    write.table(pidMap, file = outfilepath, sep = '\t', row.names = FALSE)

    cohorts <- cohorts[, c('new_person_id', 'cohort_id')]
    outfilepath <- file.path(covariatesdir, "cohorts.csv")
    write.table(cohorts, file = outfilepath, sep = '\t', row.names = FALSE)

    pidMap
}


convertCovariates <- function(datadir, covariatesdir, pidMap, topN) {
    if (is.null(topN)) {
        topN <- 1e10
    }

    reqoutfilepath <- file.path(covariatesdir, "covariates.csv")
    optoutfilepath <- file.path(covariatesdir, "optionalcovariates.csv")
    covmapoutfilepath <- file.path(covariatesdir, "covariateMap.csv")

    reqdir <- file.path(datadir, "dimensions", "required")
    optdir <- file.path(datadir, "dimensions", "optional")
    reqinpaths <- list.files(reqdir, full.names=TRUE)
    optinpaths <- list.files(optdir, full.names=TRUE)

    # Write header for covariate map.
    header <- sprintf("%s\t%s\t%s\t%s", "new_covariate_id", "old_covariate_id",
                      "level", "dim_name")
    outfile <- file(covmapoutfilepath, "w")
    writeLines(header, outfile)
    close(outfile)

    # Write headers for covariates.
    header <- sprintf("%s\t%s\t%s", "new_person_id", "new_covariate_id",
                      "new_covariate_value")
    for (outfilepath in c(reqoutfilepath, optoutfilepath)) {
        outfile <- file(outfilepath, "w")
        writeLines(header, outfile)
        close(outfile)
    }

    newbasecovid <- 1
    for (infilepath in c(reqinpaths, optinpaths)) {
        dimname <- file_path_sans_ext(basename(infilepath))
        required <- (infilepath %in% reqinpaths)
        if (required) {
            outfilepath <- reqoutfilepath
        } else {
            outfilepath <- optoutfilepath
        }

        covariates <- read.table(infilepath, header = TRUE, sep = '\t',
                                 col.names = c("old_person_id",
                                 "old_covariate_id", "old_covariate_value"),
                                 colClasses = c(old_person_id="character",
                                 old_covariate_id="character",
                                 old_covariate_value="numeric"))

        covariates <- merge(pidMap, covariates)
        covariates <- covariates[, c('new_person_id', 'old_covariate_id',
                                     'old_covariate_value')]

        # Use topN to drop covariates.
        covcounts <- aggregate(old_covariate_value ~ old_covariate_id,
                               covariates, length)
        covcounts <- covcounts[order(-covcounts$old_covariate_value),]
        localcutoff <- min(topN, length(covcounts$old_covariate_id))
        covstokeep <- covcounts[1:localcutoff,]$old_covariate_id
        covariates <- covariates[covariates$old_covariate_id %in% covstokeep,]


        # Get median and 75th percentile values.
        mids <- aggregate(old_covariate_value ~ old_covariate_id, covariates,
                          median)
        mids <- setNames(mids, c("old_covariate_id", "medians"))
        covariates <- merge(covariates, mids)
        highs <- aggregate(old_covariate_value ~ old_covariate_id, covariates,
                           seventyfifth)
        highs <- setNames(highs, c("old_covariate_id", "seventyfifth"))
        covariates <- merge(covariates, highs)

        # Set the correct level for each covariate.
        covariates$level <- rep("l", length(covariates$new_person_id))
        mask <- covariates$old_covariate_value >= covariates$medians
        covariates$level[mask] <- "m"
        mask <- covariates$old_covariate_value >= covariates$seventyfifth
        covariates$level[mask] <- "h"

        # Get new covariate ids.
        covMap <- data.frame(old_covariate_id=covariates$old_covariate_id,
                             level=covariates$level)
        covMap <- unique(covMap)
        covMap$dim_name <- rep(dimname, length(nrow(covMap)))
        finalnewcovid <- newbasecovid + nrow(covMap) - 1
        covMap$new_covariate_id <- newbasecovid:finalnewcovid
        covMap <- covMap[c("new_covariate_id", "old_covariate_id",
                           "level", "dim_name")]

        # Write out covariate map.
        write.table(covMap, file = covmapoutfilepath, append = TRUE,
                    sep = '\t', row.names = FALSE, col.names = FALSE)

        # Increase new covariate counter for next round.
        newbasecovid <- finalnewcovid + 1

        # Transform new covariates.
        covariates <- merge(covariates, covMap)
        covariates <- covariates[c('new_person_id', 'new_covariate_id')]
        covariates$new_covariate_value <- rep(1, nrow(covariates))

        # Write out covariates.
        write.table(covariates, file = outfilepath, append = TRUE,
                    sep = '\t', row.names = FALSE, col.names = FALSE)
    }
}


seventyfifth <- function(values) {
    pos <- 3*length(values)/4
    mean(c(values[floor(pos)], values[ceiling(pos)]))
}

# @file DataLoading.R
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
getSparseData <- function(covariatesdir) {
    covariatesfile <- file.path(covariatesdir, "covariates.csv")

    cohortsfile <- file.path(covariatesdir, "cohorts.csv")

    covariates <- read.table(covariatesfile, header = TRUE, sep = '\t',
                             col.names = c("new_person_id",
                             "new_covariate_id", "new_covariate_value"),
                             colClasses = c(new_person_id="numeric",
                             new_covariate_id="numeric",
                             new_covariate_value="numeric"))

    cohorts <- read.table(cohortsfile, header = TRUE, sep = '\t', col.names =
                          c("new_person_id", "cohort_id"), colClasses =
                          c(new_person_id="numeric", cohort_id="numeric"))

    # TODO: This works because it is assumed that the persons are labeled
    # consecutively without any holes. This should probably be enforced through
    # checks somewhere or incomprehensible errors might be thrown.
    X <- sparseMatrix(i = covariates$new_person_id,
                      j = covariates$new_covariate_id,
                      x = covariates$new_covariate_value)

    # TODO: This should already be ordered, but it is essential that it be so.
    # This should probably be enforced with a check somewhere.
    cohorts <- cohorts[order(cohorts$new_person_id),]
    y <- cohorts$cohort_id

    sparseData <- list(X = X, y = y)
    sparseData
}

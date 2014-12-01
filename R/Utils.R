# @file Utils.R
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
readfile <- function(filepath) {
    text <- readChar(filepath, file.info(filepath)$size)
    text
}


tutil.log <- function(text) {
    f = file(filepath, open="a")
    write(text, f)
    close(f)
}

#' @export
Logger <- setRefClass(
    "Logger",
    fields=list(
        filepath = "character"
    ),
    methods=list(
        initialize = function(filepath, truncate = FALSE) {
            callSuper(filepath = filepath)
            if (truncate) {
                # Remove old data.
                f = file(filepath, open = "w")
                close(f)
            }
        },
        log = tutil.log
    )
)

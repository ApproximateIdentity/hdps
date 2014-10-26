#' @export
readfile <- function(filepath) {
    text <- readChar(filepath, file.info(filepath)$size)
    text
}

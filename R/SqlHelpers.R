loadSql <- function(filename) {
    filepath <- system.file(
        paste("sql/sql_server/", filename, sep = ""),
        package="hdps")
    parameterizedSql <- readChar(filepath, file.info(filepath)$size)  
    return(parameterizedSql)
}


#' @export
loadLocalSql <- function(filepath) {
    parameterizedSql <- readChar(filepath, file.info(filepath)$size)
    return(parameterizedSql)
}

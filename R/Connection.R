# This is necessary due to some mystical shadowing of 'connect' which occurs
# later.
DatabaseConnector.connect <- connect

Connection.connect <- function() {
    conn <<- DatabaseConnector.connect(
        dbms=dbms,
        user,
        password,
        server,
        port,
        schema)
    connected <<- TRUE
}


Connection.disconnect <- function() {
    dbDisconnect(conn)
    connected <<- FALSE
}


Connection.execute <- function(renderedSql) {
    translatedSql <- translateSql(renderedSql, "sql server", dbms)$sql
    executeSql(conn, translatedSql)
}


Connection.executeforresult <- function(renderedSql) {
    translatedSql <- translateSql(renderedSql, "sql server", dbms)$sql
    result <<- dbSendQuery(conn, translatedSql)
    dbFetch(result)
}


#' @export
Connection <- setRefClass(
    "Connection",
    fields=list(
        password = "character",
        dbms = "character",
        user = "character",
        server = "character",
        # Should cdmSchema be here?
        schema = "character",
        port = "character",
        connected = "logical",
        conn = "ANY",
        result = "ANY"
    ),
    prototype=list(
        connected = FALSE
    ),
    methods=list(
        initialize = function(...) {
          callSuper(...)
        },
        connect = Connection.connect,
        disconnect = Connection.disconnect,
        execute = Connection.execute,
        executeforresult = Connection.executeforresult
    )
)

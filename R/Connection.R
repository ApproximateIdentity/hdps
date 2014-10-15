# This is necessary due to some mystical shadowing of 'connect' which occurs
# later.
DatabaseConnector.connect <- connect

Connection.connect <- function(dbms, user, password, server, port, schema) {
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
    if (debug) {
        logger$log(translatedSql)
        print("debug mode")
    } else {
        executeSql(conn, translatedSql)
    }
}


Connection.executeforresult <- function(renderedSql) {
    translatedSql <- translateSql(renderedSql, "sql server", dbms)$sql
    if (debug) {
        logger$log(translatedSql)
        print("debug mode")
        return(NULL)
    } else {
        result <<- dbSendQuery(conn, translatedSql)
        return(dbFetch(result))
    }
}


#' @export
Connection <- setRefClass(
    "Connection",
    fields=list(
        schema = "ANY",
        connected = "ANY",
        conn = "ANY",
        result = "ANY",
        logger = "ANY",
        debug = "ANY"
    ),
    prototype=list(
        connected = FALSE,
        debug = FALSE
    ),
    methods=list(
        initialize = function(...) {
            callSuper(...)
            connected <<- FALSE
            debug <<- FALSE
            logger <<- Logger$new(filepath="/tmp/sqlqueries.log")
        },
        connect = Connection.connect,
        disconnect = Connection.disconnect,
        execute = Connection.execute,
        executeforresult = Connection.executeforresult
    )
)

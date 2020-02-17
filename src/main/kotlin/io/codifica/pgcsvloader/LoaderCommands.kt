package io.codifica.pgcsvloader

import org.postgresql.jdbc.PgConnection
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod

@ShellComponent
class LoaderCommands {

    @ShellMethod("Add two integers together.")
    fun add(a: Int, b: Int): Int {
        println()
        return a + b
    }

    @ShellMethod("Load csv file to non-existing Postgres table")
    fun load(file: String, host: String, port: Int, database: String, table: String, username: String, password: String) {
        val pgClient = PgClient(host, port, database, username, password)

        val pair = CsvParser().parse(file, table)
        val createTableTemplate = pair.first
        val reader = pair.second

        pgClient.connection
                .createStatement()
                .execute(createTableTemplate)

        val copyStatement = "COPY import.$table FROM STDIN DELIMITER ',' CSV HEADER"
        val pgConnection = pgClient.connection as PgConnection
        pgConnection.copyAPI.copyIn(copyStatement, reader)
    }

}
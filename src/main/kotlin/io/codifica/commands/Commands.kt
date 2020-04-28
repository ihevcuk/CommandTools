package io.codifica.commands

import io.codifica.commands.kafka.KafkaMessageSeek
import io.codifica.commands.postgres.CsvParser
import io.codifica.commands.postgres.PgClient
import org.postgresql.jdbc.PgConnection
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import java.io.FileWriter
import java.util.*

@ShellComponent
class Commands {

    @ShellMethod("Load csv file to non-existing Postgres table")
    fun load(file: String, host: String, port: Int, database: String, table: String, username: String, password: String) {
        println("bok")
        val pgClient = PgClient(host, port, database, username, password)

        val pair = CsvParser().parse(file, table)
        val createTableTemplate = pair.first
        val reader = pair.second

        pgClient.connection
                .createStatement()
                .execute(createTableTemplate)

        val copyStatement = "COPY $table FROM STDIN DELIMITER ',' CSV HEADER"
        val pgConnection = pgClient.connection as PgConnection
        pgConnection.copyAPI.copyIn(copyStatement, reader)

        pgClient.connection.close()
    }

    @ShellMethod("Export data to csv file from Postgres table")
    fun export(file: String, host: String, port: Int, database: String, table: String, username: String, password: String) {
        val pgClient = PgClient(host, port, database, username, password)

        val copyStatement = "COPY (SELECT * FROM $table) TO STDOUT DELIMITER ',' CSV"
        val pgConnection = pgClient.connection as PgConnection

        val fileWriter = FileWriter(file)
        pgConnection.copyAPI.copyOut(copyStatement, fileWriter)

        fileWriter.flush()
        fileWriter.close()

        pgClient.connection.close()
    }

    @ShellMethod("Seek Kafka topic")
    fun seek(server: String, topic: String, sinceUtc: String, untilUtc: String, partitions: Int, grep: String = "") {
        val kafkaMessageSeek = KafkaMessageSeek(server, topic, sinceUtc, untilUtc, partitions, grep)
        kafkaMessageSeek.seek()
    }

}
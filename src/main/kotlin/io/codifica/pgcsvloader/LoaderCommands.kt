package io.codifica.pgcsvloader

import org.postgresql.jdbc.PgConnection
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import java.io.FileWriter
import java.util.*

@ShellComponent
class LoaderCommands {
    val statsFinder : StatsFinder

    constructor(statsFinder: StatsFinder) {
        this.statsFinder = statsFinder
    }

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

    @ShellMethod("Fetch stats for tennis player")
    fun stats(player: String) : String {
        val prompt = Scanner(System.`in`)

        val players = this.statsFinder.find(player)
        //players.forEach { p -> println(p) }

        if (players.size == 1) {
            print("Did you mean on ${players[0].label}? Y/N: ")

            val isCorrectPlayerFound = prompt.nextLine().equals("y", true)
            if (isCorrectPlayerFound) {
                println("Good!")
            }
            else {
                println("I can't help you then")
            }
        }
        else {
            println("Please select number of correct player from the list: ")
            players.forEachIndexed { index, player -> println("${index + 1}) ${player.label}") }
            print("Selection: ")

            val playerIndex = prompt.nextInt() - 1

            println("Selected player is ${players[playerIndex]}")
        }

        return "Bye!"
    }

    @ShellMethod("Seek Kafka topic")
    fun seek(server: String, topic: String, sinceUtc: String, untilUtc: String, partitions: Int, grep: String = "") {
//        val kafkaMessageSeek = KafkaMessageSeek(
//                "localhost:29092",
//                "find",
//                "2020-04-15T11:00:26",
//                "2020-04-18T09:51:41",
//                3,
//                null
//        )

        val kafkaMessageSeek = KafkaMessageSeek(server, topic, sinceUtc, untilUtc, partitions, grep)
        kafkaMessageSeek.seek()
    }

}
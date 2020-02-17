package io.codifica.pgcsvloader

import java.sql.Connection
import java.sql.DriverManager
import java.util.*


class PgClient {
    val connection: Connection

    constructor(host: String, port: Int, database: String, username: String, password: String) {
        val url = "jdbc:postgresql://$host:$port/$database"
        val props = Properties()
        props.setProperty("user", username)
        props.setProperty("password", password)
        this.connection = DriverManager.getConnection(url, props)
    }

}
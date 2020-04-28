package io.codifica.commands.postgres

class PgCreateTableTemplate {

    fun create(columns: Array<String>, table: String) : String {
        var columnsSpecification = columns.joinToString(",") { c -> "$c TEXT" }
        return "CREATE TABLE IF NOT EXISTS $table ($columnsSpecification);"
    }

}
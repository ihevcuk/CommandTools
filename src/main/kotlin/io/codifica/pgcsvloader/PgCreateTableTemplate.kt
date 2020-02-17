package io.codifica.pgcsvloader

class PgCreateTableTemplate {

    fun create(columns: Array<String>, table: String, schema: String = "import") : String {
        var columnsSpecification = columns.joinToString(",") { c -> "$c TEXT" }
        return "CREATE TABLE IF NOT EXISTS $schema.$table ($columnsSpecification);"
    }

}
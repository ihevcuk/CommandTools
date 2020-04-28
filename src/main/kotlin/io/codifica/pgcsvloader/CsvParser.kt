package io.codifica.pgcsvloader

import com.opencsv.CSVReader
import java.io.FileReader
import java.io.Reader

class CsvParser {

    fun parse(file: String, table: String) : Pair<String, Reader> {
        val reader = CSVReader(FileReader(file))

        var line = reader.readNext()
        var createTableSql = PgCreateTableTemplate().create(line, table)

        return Pair(createTableSql, FileReader(file))
    }

}

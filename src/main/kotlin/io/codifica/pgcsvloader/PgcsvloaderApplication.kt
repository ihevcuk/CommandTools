package io.codifica.pgcsvloader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PgcsvloaderApplication

fun main(args: Array<String>) {
	runApplication<PgcsvloaderApplication>(*args)
}

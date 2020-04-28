package io.codifica.commands

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PgcsvloaderApplication

fun main(args: Array<String>) {
	runApplication<PgcsvloaderApplication>(*args)
}

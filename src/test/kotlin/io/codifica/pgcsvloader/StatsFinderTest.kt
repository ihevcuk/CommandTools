package io.codifica.pgcsvloader

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

internal class StatsFinderTest {

    @Test
    fun searchApi() {
//        StatsFinder().find("djokovic")
//                .forEach { p -> println(p) }

        val parameters = mapOf("playerId" to 1, "surface" to "H")

        val params = parameters.asIterable().joinToString("&") { entry -> "${entry.key}=${entry.value}" }
        println(params)
    }
}
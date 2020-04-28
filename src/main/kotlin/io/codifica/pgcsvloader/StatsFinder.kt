package io.codifica.pgcsvloader

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.springframework.stereotype.Service
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Service
class StatsFinder {
    val objectMapper = jacksonObjectMapper()

    fun find(player: String) : Array<Player> {
        val client = HttpClient.newBuilder().build();
        val request = HttpRequest.newBuilder()
                .uri(URI.create("https://www.ultimatetennisstatistics.com/autocompletePlayer?term=$player"))
                .build()
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())

        return this.objectMapper.readValue(response.body())
    }

    fun stats(player: Player, surface: String) : String {
        val url = "https://www.ultimatetennisstatistics.com/playerStatsTab?"
        val parameters = mapOf("playerId" to player.id, "surface" to surface)
                            .asIterable().joinToString("&") { entry -> "${entry.key}=${entry.value}" }
        val document = Jsoup.connect("$url$parameters").get()

        val elements: Elements = document.select("table.mega-table tbody tr td")

        return ""
    }

}
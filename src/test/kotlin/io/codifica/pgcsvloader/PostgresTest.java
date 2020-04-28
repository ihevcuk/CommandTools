package io.codifica.pgcsvloader;

import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresTest {

    @Test
    public void copy() throws SQLException, IOException {
        PgClient pgClient = new PgClient("192.168.113.134", 5441, "telemetryreport", "telemetryreport","telemetryreport");

        PgConnection pgConnection =  (PgConnection) pgClient.getConnection();

        StringBuffer buffer = new StringBuffer();
        buffer.append("9, ").append("tip").append(System.lineSeparator());

        StringReader stringReader = new StringReader(buffer.toString());
        pgConnection.getCopyAPI().copyIn("COPY public.telemetry(device_id, device_name) FROM STDIN DELIMITER ','", stringReader);

        pgConnection.close();
    }


}

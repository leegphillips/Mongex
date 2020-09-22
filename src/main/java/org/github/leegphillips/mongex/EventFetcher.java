package org.github.leegphillips.mongex;

import org.bson.BsonArray;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

public class EventFetcher {
    public BsonArray fetch(String command) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
        Process process = processBuilder.start();
        InputStream inputStream = process.getInputStream();
        String text = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines()
                .collect(joining("\n"));
        return process.exitValue() == 0 ? BsonArray.parse(text) : new BsonArray();
    }
}

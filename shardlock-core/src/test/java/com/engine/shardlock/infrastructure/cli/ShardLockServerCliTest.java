package com.engine.shardlock.infrastructure.cli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ShardLock Server CLI Unit Tests")
class ShardLockServerCliTest {

    @Test
    @DisplayName("Should print version with --version, -v, and version")
    void shouldPrintVersion() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(out));
            ShardLockServerCli.main(new String[]{"--version"});
            assertThat(out.toString()).contains("shardlock-core version 1.0.0");

            out.reset();
            ShardLockServerCli.main(new String[]{"-v"});
            assertThat(out.toString()).contains("shardlock-core version 1.0.0");

            out.reset();
            ShardLockServerCli.main(new String[]{"version"});
            assertThat(out.toString()).contains("shardlock-core version 1.0.0");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    @DisplayName("Should print help with --help and -h")
    void shouldPrintHelp() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(out));
            ShardLockServerCli.main(new String[]{"--help"});
            assertThat(out.toString()).contains("Usage:").contains("--node-id");

            out.reset();
            ShardLockServerCli.main(new String[]{"-h"});
            assertThat(out.toString()).contains("Usage:").contains("--node-id");
        } finally {
            System.setOut(originalOut);
        }
    }
}

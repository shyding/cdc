package com.cdc.student.config;

import org.apache.kafka.connect.json.JsonConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DebeziumConnectorConfig {

    
    @Value("${student.datasource.host}")
    private String studentDBHost;

    @Value("${student.datasource.databasename}")
    private String studentDBName;

    @Value("${student.datasource.port}")
    private String studentDBPort;

    @Value("${student.datasource.username}")
    private String studentDBUserName;

    @Value("${student.datasource.password}")
    private String studentDBPassword;

    private String STUDENT_TABLE_NAME = "student";

    /**
     * Student database connector.
     *
     * @return Configuration.
     */
    @Bean
    public io.debezium.config.Configuration studentConnector() {
        return io.debezium.config.Configuration.create()
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage",  "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "/path/to/data/student-offset.dat")
                .with("offset.flush.interval.ms", 60000)
				.with("database.serverTimezone", "UTC")
//				.with("plugin.name", "pgoutput")
                .with("name", "student-postgres-connector")
                .with("database.server.name", studentDBHost+"-"+studentDBName)
                .with("database.server.id", 1)
                .with("database.hostname", studentDBHost)
                .with("database.port", studentDBPort)
                .with("database.user", studentDBUserName)
                .with("database.password", studentDBPassword)
//                .with("database.dbname", studentDBName)
                .with("database.whitelist", studentDBName)
//                .with("table.whitelist", STUDENT_TABLE_NAME)
                .with("database.history",
                        "io.debezium.relational.history.FileDatabaseHistory")
                  .with("database.history.file.filename",
                        "/path/to/storage/dbhistory.dat")
                .build();
    }
    
    @Bean
    public JsonConverter keyConverter(io.debezium.config.Configuration embeddedConfig) {
        JsonConverter converter = new JsonConverter();
        converter.configure(embeddedConfig.asMap(), true);
        return converter;
    }

    @Bean
    public JsonConverter valueConverter(io.debezium.config.Configuration embeddedConfig) {
        JsonConverter converter = new JsonConverter();
        converter.configure(embeddedConfig.asMap(), false);
        return converter;
    }
}

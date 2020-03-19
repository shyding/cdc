package com.cdc.student.listener;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cdc.student.service.StudentService;
import com.cdc.student.sql.AbstractDebeziumSqlProvider;
import com.cdc.student.sql.DebeziumSqlProviderFactory;
import com.cdc.student.utils.DebeziumRecordUtils;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngine;
import lombok.extern.slf4j.Slf4j;

/**
 * This class creates, starts and stops the EmbeddedEngine, which starts the Debezium engine. The engine also
 * loads and launches the connectors setup in the configuration.
 * <p>
 * The class uses @PostConstruct and @PreDestroy functions to perform needed operations.
 *
 * @author Sohan
 */
@Slf4j
@Component
public class CDCListener {

    /**
     * Single thread pool which will run the Debezium engine asynchronously.
     */
    private final Executor executor = Executors.newSingleThreadExecutor();

    /**
     * The Debezium engine which needs to be loaded with the configurations, Started and Stopped - for the
     * CDC to work.
     */
    private final EmbeddedEngine engine;

    /**
     * Handle to the Service layer, which interacts with ElasticSearch.
     */
    private final StudentService studentService;

    /**
     * Constructor which loads the configurations and sets a callback method 'handleEvent', which is invoked when
     * a DataBase transactional operation is performed.
     *
     * @param studentConnector
     * @param studentService
     */
    private CDCListener(Configuration studentConnector, StudentService studentService) {
        this.engine = EmbeddedEngine
                .create()
                .using(studentConnector)
                .notifying(this::handleRecord).build();

        this.studentService = studentService;
    }

    /**
     * The method is called after the Debezium engine is initialized and started asynchronously using the Executor.
     */
    @PostConstruct
    private void start() {
        this.executor.execute(engine);
    }

    /**
     * This method is called when the container is being destroyed. This stops the debezium, merging the Executor.
     */
    @PreDestroy
    private void stop() {
        if (this.engine != null) {
            this.engine.stop();
        }
    }

   
    /**
     * For every record this method will be invoked.
     */
    private void handleRecord(SourceRecord record) {
        logRecord(record);

        Struct payload = (Struct) record.value();
        if (Objects.isNull(payload)) {
            return;
        }
        String table = Optional.ofNullable(DebeziumRecordUtils.getRecordStructValue(payload, "source"))
                .map(s->s.getString("table")).orElse(null);

//        // 处理数据DML
        Envelope.Operation operation = DebeziumRecordUtils.getOperation(payload);
        if (Objects.nonNull(operation)) {
            Struct key = (Struct) record.key();
            handleDML(key, payload, table, operation);
            return;
        }
//
//        // 处理结构DDL
        String ddl = getDDL(payload);
        if (StringUtils.isNotBlank(ddl)) {
            handleDDL(ddl);
        }
    }

    private String getDDL(Struct payload) {
        String ddl = DebeziumRecordUtils.getDDL(payload);
        log.info("ddl:{}" ,ddl);
        if (StringUtils.isBlank(ddl)) {
            return null;
        }
        String db = DebeziumRecordUtils.getDatabaseName(payload);
        if (StringUtils.isBlank(db)) {
           log.info("db:{}" ,db);
        }
        ddl = ddl.replace(db + ".", "");
        ddl = ddl.replace("`" + db + "`.", "");
        return ddl;
    }

    /**
     * 执行数据库ddl语句
     *
     * @param ddl
     */
    private void handleDDL(String ddl) {
        log.info("ddl语句 : {}", ddl);
        try {
         
        } catch (Exception e) {
            log.error("数据库操作DDL语句失败，", e);
        }
    }

    /**
     * 处理insert,update,delete等DML语句
     *
     * @param key       表主键修改事件结构
     * @param payload   表正文响应
     * @param table     表名
     * @param operation DML操作类型
     */
    private void handleDML(Struct key, Struct payload, String table, Envelope.Operation operation) {
        AbstractDebeziumSqlProvider provider = DebeziumSqlProviderFactory.getProvider(operation);
        if (Objects.isNull(provider)) {
            log.error("没有找到sql处理器提供者.");
            return;
        }

        String sql = provider.getSql(key, payload, table);
        if (StringUtils.isBlank(sql)) {
            log.error("找不到sql.");
            return;
        }

        try {
            log.info("dml语句 : {}", sql);
            log.info("parm:{}" ,  provider.getSqlParameterMap());
         
        } catch (Exception e) {
            log.error("数据库DML操作失败，", e);
        }
    }

    
    @Autowired
    private JsonConverter keyConverter;

    @Autowired
    private JsonConverter valueConverter;
    
    /**
     * 打印消息
     *
     * @param record
     */
    private void logRecord(SourceRecord record) {
        final byte[] payload = valueConverter.fromConnectData("dummy", record.valueSchema(), record.value());
        final byte[] key = keyConverter.fromConnectData("dummy", record.keySchema(), record.key());
        log.info("Publishing Topic --> {}", record.topic());
        log.info("Key --> {}", new String(key));
        log.info("Payload --> {}", new String(payload));
    }
}
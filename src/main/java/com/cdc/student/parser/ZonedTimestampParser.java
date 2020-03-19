package com.cdc.student.parser;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.connect.data.Schema;

import lombok.extern.slf4j.Slf4j;

/**
 * 功能：
 *
 * @author Damon
 * @since 2019-04-22 11:06
 */
@Slf4j
public class ZonedTimestampParser implements DebeziumParser<Object, String> {

    @Override
    public String parse(Schema schema, Object input) {
        try {
            Date date = DateFormatUtils.ISO_DATETIME_FORMAT.parse(input.toString());
            return DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
        } catch (ParseException e) {
            log.error("解析ZonedTimestamp失败，", e);
        }
        return null;
    }
}

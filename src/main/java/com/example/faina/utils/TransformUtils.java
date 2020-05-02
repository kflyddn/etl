package com.example.faina.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransformUtils {

    private static CsvSchema csv = CsvSchema.emptySchema().withHeader();
    private static CsvMapper csvMapper = new CsvMapper();

    public static String csvToJson(String csvString) throws IOException {
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(csvString);
        List<Map<?,?>> res = mappingIterator.readAll();
        //TODO: remove surrounding []
        return res.toString();
    }

    public static String xmlToJson(ConsumerRecord<?, ?> cr) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(cr.value().toString().getBytes());
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.writeValueAsString(node);
    }
}

package com.example.faina.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

//TODO: create interface Transformer,
// class XmlTransformer implements Transformer, class CsvTransformer implements Transformer
//TODO: move the transformation to XmlTransformer.transform(), CsvTransformer.transform()
public class TransformUtils {

    private static CsvSchema csv = CsvSchema.emptySchema().withHeader();
    private static CsvMapper csvMapper = new CsvMapper();
    private static XmlMapper xmlMapper = new XmlMapper();
    private static ObjectMapper jsonMapper = new ObjectMapper();


    //TODO: make sure that all transformer methods return object of the same class
    public static JSONObject csvToJson(String csvString) throws IOException {
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(csvString);
        List<Map<?,?>> res = mappingIterator.readAll();
        if (res != null && res.size() > 0) {
            return new JSONObject(res.get(0));
        }

        return new JSONObject();

    }

    //TODO: make sure that all transformer methods return object of the same class
    private static JsonNode xmlToJson(ConsumerRecord<?, ?> cr) throws IOException {

        JsonNode node = xmlMapper.readTree(cr.value().toString().getBytes());
        return node;
    }

    public static String xmlToJsonString(ConsumerRecord<?, ?> cr) throws IOException {
        return jsonMapper.writeValueAsString(xmlToJson(cr));
    }
}

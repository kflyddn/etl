package com.example.faina.utils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CsvUtils {

    private static CsvSchema csv = CsvSchema.emptySchema().withHeader();
    private static CsvMapper csvMapper = new CsvMapper();

    public static String csvToJson(String csvString) throws IOException {
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(csvString);
        List<Map<?,?>> res = mappingIterator.readAll();
        //TODO: remove surrounding []
        return res.toString();
    }
}

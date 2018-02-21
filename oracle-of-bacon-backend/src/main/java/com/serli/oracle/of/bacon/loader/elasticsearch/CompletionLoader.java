package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];

        Stack<BulkRequest> brStack = new Stack<>();
        brStack.push(new BulkRequest());
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                        .forEach((String line) -> {

                            // Packaging IndexRequest by 5MB's BulkRequest
                            if(brStack.peek().estimatedSizeInBytes() > 5242880) {
                                brStack.push(new BulkRequest());
                            }

                            Map<String, Object> jsonMap = new HashMap<>();
                            jsonMap.put("fullname", line);
                            brStack.peek().add(
                             new IndexRequest("imdb", "actor").source(jsonMap)
                            );

            });
        }

        while(!brStack.empty()) {
            count.addAndGet(client.bulk(brStack.pop()).getItems().length);
            System.out.println("... inserted actors: " + count);
        }

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }
}

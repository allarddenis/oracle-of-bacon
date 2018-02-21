package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import javax.sound.sampled.Port;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static final String INDEX_IMDB = "imdb";
    private static final String TYPE_ACTOR = "actor";

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(INDEX_IMDB);
            client.indices().delete(request);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                System.out.println("IMDB index's already purged.");
            }
        }

        CreateIndexRequest request = new CreateIndexRequest(INDEX_IMDB);
        request.mapping(TYPE_ACTOR,
                "  {\n" +
                        "    \"" + TYPE_ACTOR +"\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"suggestion\": {\n" +
                        "          \"type\": \"completion\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }",
                XContentType.JSON);
        client.indices().create(request);

        String inputFilePath = args[0];
        Stack<BulkRequest> brStack = new Stack<>();
        brStack.push(new BulkRequest());
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                        .forEach((String line) -> {

                            line = line.replace("\"", "").replace(",", "");
                            System.out.println(line);

                            // Packaging IndexRequest by 5MB's BulkRequest
                            if(brStack.peek().estimatedSizeInBytes() > 5242880) {
                                brStack.push(new BulkRequest());
                            }

                            Map<String, Object> jsonMap = new HashMap<>();
                            jsonMap.put("fullname", line);
                            jsonMap.put("suggestion", combinations(line.split(" ")));
                            brStack.peek().add(
                             new IndexRequest(INDEX_IMDB, TYPE_ACTOR).source(jsonMap)
                            );

            });
        }

        while(!brStack.empty()) {
            count.addAndGet(client.bulk(brStack.pop()).getItems().length);
            System.out.println("... current inserted actors counter: " + count);
        }

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }

    public static String[] combinations (String[] actors ) {
        List<String> combinationList = new ArrayList<String>();
        // Start i at 1, so that we do not include the empty set in the results
        for ( long i = 1; i < Math.pow(2, actors.length); i++ ) {
            String combination = "";
            for ( int j = 0; j < actors.length; j++ ) {
                if ( (i & (long) Math.pow(2, j)) > 0 ) {
                    // Include j in set
                    if(!combination.isEmpty())
                        combination += " ";
                    combination += actors[j];
                }
            }
            combinationList.add(combination);
        }
        return combinationList.toArray(new String[0]);
    }

}

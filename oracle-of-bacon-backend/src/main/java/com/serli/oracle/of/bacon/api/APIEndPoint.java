package com.serli.oracle.of.bacon.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
import net.codestory.http.annotations.Get;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {

        List<?> list = neo4JRepository.getConnectionsToKevinBacon(actorName);
        JsonArray json = new JsonArray();

        for(Node n: (Iterable<? extends Node>) list.get(0))
        {
            JsonObject jsonObj = new JsonObject();

            JsonObject node = new JsonObject();
            node.addProperty("id", n.id());
            node.addProperty("type", n.labels().iterator().next());
            node.addProperty("value", n.values().iterator().next().toString());
            jsonObj.add("data", node);

            json.add(jsonObj);
        }

        for(Relationship r: (Iterable<? extends Relationship>) list.get(1))
        {
            JsonObject jsonObj = new JsonObject();

            JsonObject relationship = new JsonObject();
            relationship.addProperty("id", r.id());
            relationship.addProperty("source", r.startNodeId());
            relationship.addProperty("target", r.endNodeId());
            relationship.addProperty("value", r.type());
            jsonObj.add("data", relationship);

            json.add(jsonObj);
        }

        redisRepository.setLastSearch(actorName);
        return json.toString();
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return Arrays.asList("Niro, Chel",
                "Senanayake, Niro",
                "Niro, Juan Carlos",
                "de la Rua, Niro",
                "Niro, Simão");
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        return "{\n" +
                "\"_id\": {\n" +
                "\"$oid\": \"587bd993da2444c943a25161\"\n" +
                "},\n" +
                "\"imdb_id\": \"nm0000134\",\n" +
                "\"name\": \"Robert De Niro\",\n" +
                "\"birth_date\": \"1943-08-17\",\n" +
                "\"description\": \"Robert De Niro, thought of as one of the greatest actors of all time, was born in Greenwich Village, Manhattan, New York City, to artists Virginia (Admiral) and Robert De Niro Sr. His paternal grandfather was of Italian descent, and his other ancestry is Irish, German, Dutch, English, and French. He was trained at the Stella Adler Conservatory and...\",\n" +
                "\"image\": \"https://images-na.ssl-images-amazon.com/images/M/MV5BMjAwNDU3MzcyOV5BMl5BanBnXkFtZTcwMjc0MTIxMw@@._V1_UY317_CR13,0,214,317_AL_.jpg\",\n" +
                "\"occupation\": [\n" +
                "\"actor\",\n" +
                "\"producer\",\n" +
                "\"soundtrack\"\n" +
                "]\n" +
                "}";
    }
}

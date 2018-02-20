package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Optional;

public class MongoDbRepository {
    private final MongoCollection<Document> actorCollection;

    public MongoDbRepository() {
        this.actorCollection= new MongoClient("localhost", 27017).getDatabase("workshop").getCollection("actors");
    }

    public Optional<Document> getActorByName(String name) {

        Optional<Document> document = Optional.empty();

        for (Document doc: actorCollection.find()) {
            if(doc.containsKey("name") && doc.get("name").equals(name))
            {
                document = Optional.of(doc);
            }
        }

        return document;
    }
}

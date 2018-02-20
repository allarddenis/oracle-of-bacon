package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;

import com.github.sommeri.less4j.core.parser.LessParser.variabledeclaration_return;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "dede"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {

        String req = "MATCH p=shortestPath((bacon:Actor {name:\"Bacon, Kevin (I)\"})-[*]-(actor:Actor {name:\"" + actorName + "\"})) RETURN DISTINCT p";

        /*
        StringBuilder sb = new StringBuilder();
        sb.append("MATCH ({name:\"");
        sb.append(actorName);
        sb.append("\"})-[:PLAYED_IN]->(films)<-[:PLAYED_IN]-(actors) WHERE NOT actors.name=\"");
        sb.append(actorName);
        sb.append("\" RETURN DISTINCT actors.name");
        */

        ArrayList<Object> results = new ArrayList<Object>(){};
        
        try ( Session session = driver.session() )
        {
            StatementResult rs = session.run(req);

            Path path = rs.next().values().get(0).asPath();
            results.add(path.nodes());
            results.add(path.relationships());

            return results;
        }
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}

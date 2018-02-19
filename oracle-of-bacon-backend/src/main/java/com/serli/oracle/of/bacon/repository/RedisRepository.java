package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private final static String LAST_SEARCHES_KEY = "lastsearches";

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange(LAST_SEARCHES_KEY, 0, 10);
    }

    public void setLastSearch(String actorName) {
        jedis.lpush(LAST_SEARCHES_KEY, actorName);
    }
}

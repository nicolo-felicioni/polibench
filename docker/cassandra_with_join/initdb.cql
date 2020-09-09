CREATE KEYSPACE IF NOT EXISTS usde WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

DROP TABLE IF EXISTS usde.tweet;
DROP TABLE IF EXISTS usde.user;
DROP TABLE IF EXISTS usde.engagement;

CREATE TABLE usde.user
(
    id TEXT,
    verified BOOLEAN,
    PRIMARY KEY (id)
);

CREATE TABLE usde.tweet
(
    id TEXT,
    language TEXT,
    creation_timestamp INT,
    creator TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS tweet_creation_timestamp_index ON usde.tweet (creation_timestamp);
CREATE INDEX IF NOT EXISTS tweet_language_index ON usde.tweet (language);

CREATE TABLE usde.engagement
(
    tweet_id TEXT,
    engager_id TEXT,
    engagement BOOLEAN,
    PRIMARY KEY (tweet_id, engager_id)
);

CREATE TABLE usde.row
(
    tweet_id TEXT,
    engager_id TEXT,
    creator_id TEXT,
    verified BOOLEAN,
    language TEXT,
    creation_timestamp INT,
    engagement BOOLEAN,
    PRIMARY KEY (tweet_id, engager_id)
);

CREATE INDEX IF NOT EXISTS tweet_language_index ON usde.row (language);
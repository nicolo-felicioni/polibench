use benchmark_vitess;

CREATE TABLE user
(
    id VARCHAR(40),
    verified BOOLEAN,
    PRIMARY KEY (id)
);

CREATE TABLE tweet
(
    id VARCHAR(40),
    language VARCHAR(40),
    creation_timestamp LONG,
    creator VARCHAR(40),
    PRIMARY KEY (id)
);

CREATE TABLE engagement
(
    tweet_id VARCHAR(40),
    engager_id VARCHAR(40),
    engagement BOOLEAN,
    PRIMARY KEY (tweet_id, engager_id)
);
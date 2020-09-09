package common.data.entity;

public class Tweet extends DataEntity {

    // Tweet features
    private String tweetId;
    private Integer creationTimestamp;
    private String language;

    public Tweet() {}

    public Tweet(String tweetId, Integer creationTimestamp, String language) {
        this.tweetId = tweetId;
        this.creationTimestamp = creationTimestamp;
        this.language = language;
    }

    public String getTweetId() {
        return tweetId;
    }

    public Integer getCreationTimestamp() {
        return creationTimestamp;
    }

    public String getLanguage() {
        return language;
    }

    public void setTweetId(String tweetId) {
        this.tweetId = tweetId;
    }

    public void setCreationTimestamp(Integer creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}

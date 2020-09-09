package common.data;

import com.google.gson.Gson;
import common.data.entity.Engagement;
import common.data.entity.Tweet;
import common.data.entity.User;

public class DataRow {

    private Tweet tweet;
    private User engager;
    private User creator;
    private Engagement engagement;

    public DataRow() {
    }

    public DataRow(Tweet tweet, User engager, User creator, Engagement engagement) {
        this.tweet = tweet;
        this.engager = engager;
        this.creator = creator;
        this.engagement = engagement;
    }

    public static DataRow fromText(String text) {
        String[] strings = text.split("\u0001");
        String tweetId = strings[2];
        String language = strings[7];
        Integer creationTimestamp = Integer.valueOf(strings[8]);
        Tweet tweet = new Tweet(tweetId, creationTimestamp, language);
        String creatorId = strings[9];
        Boolean isCreatorVerified = Boolean.valueOf(strings[12]);
        User creator = new User(creatorId, isCreatorVerified);
        String engagerId = strings[14];
        Boolean isEngagerVerified = Boolean.valueOf(strings[17]);
        User engager = new User(engagerId, isEngagerVerified);
        // Check if there's an engagement
        Boolean eng = strings.length > 20;
        Engagement engagement = new Engagement(eng);
        return new DataRow(tweet, engager, creator, engagement);
    }

    public Tweet getTweet() {
        return tweet;
    }

    public User getEngager() {
        return engager;
    }

    public User getCreator() {
        return creator;
    }

    public Engagement getEngagement() {
        return engagement;
    }

    public void setTweet(Tweet tweet) {
        this.tweet = tweet;
    }

    public void setEngager(User engager) {
        this.engager = engager;
    }

    public void setCreator(User creator) {
        this.creator = creator;
    }

    public void setEngagement(Engagement engagement) {
        this.engagement = engagement;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

}

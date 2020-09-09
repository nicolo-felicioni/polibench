package common.data.entity;

public class Engagement extends DataEntity {

    // Engagement features
    private Boolean engagement;

    public Engagement() {}

    public Engagement(Boolean engagement) {
        this.engagement = engagement;
    }

    public Boolean getEngagement() {
        return engagement;
    }

    public void setEngagement(Boolean engagement) {
        this.engagement = engagement;
    }
}

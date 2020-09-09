package common.data.entity;

public class User extends DataEntity {

    // User features
    private String userId;
    private Boolean isVerified;

    public User() {}

    public User(String userId, Boolean isVerified) {
        this.userId = userId;
        this.isVerified = isVerified;
    }

    public String getUserId() {
        return userId;
    }

    public Boolean getVerified() {
        return isVerified;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setVerified(Boolean verified) {
        isVerified = verified;
    }
}

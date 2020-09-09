package common.data.entity;

import com.google.gson.Gson;

public abstract class DataEntity {

    public String toJson() {
        return new Gson().toJson(this);
    }

    public String toString() {
        return toJson();
    }

}

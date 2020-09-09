package common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public abstract class Configuration {

    public Configuration fromJson(String json) {
        return new Gson().fromJson(json, this.getClass());
    }

    public String toJson() {
        return new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(this);
    }

    public void save(String path) throws IOException {
        FileWriter writer = new FileWriter(path);
        new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(this, writer);
        writer.flush();
        writer.close();
    }

    public void read(String path) throws IOException {
        new Gson().fromJson(new JsonReader(new FileReader(path)), this.getClass());
    }

}

package common;

public interface Database extends Executor {

    void connect();

    void close();

    void execute(Operation operation);

    void clear();

}

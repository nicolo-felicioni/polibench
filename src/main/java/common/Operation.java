package common;

public interface Operation<T> {

    T visit(Database db);

}

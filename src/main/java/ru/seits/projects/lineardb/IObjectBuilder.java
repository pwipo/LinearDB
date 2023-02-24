package ru.seits.projects.lineardb;

public interface IObjectBuilder<T> {
    T apply(int version, byte[] bytes);
}

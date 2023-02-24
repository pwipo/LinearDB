package ru.seits.projects.lineardb;

public interface IBytesBuilder<T> {
    byte[] apply(int version, T obj);
}

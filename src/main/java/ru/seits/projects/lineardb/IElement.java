package ru.seits.projects.lineardb;

import java.util.List;

public interface IElement {

    long getId();

    long getDate();

    List<Object> getAdditionalData();

    int getVersion();
}

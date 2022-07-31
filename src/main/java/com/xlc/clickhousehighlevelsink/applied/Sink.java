package com.xlc.clickhousehighlevelsink.applied;

import java.util.concurrent.ExecutionException;

public interface Sink extends AutoCloseable {
    void put(String message) throws ExecutionException, InterruptedException;
}

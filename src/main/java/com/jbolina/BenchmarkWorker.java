package com.jbolina;

import java.util.concurrent.CompletableFuture;

public interface BenchmarkWorker {

  CompletableFuture<Boolean> submit();

  void setup();

  void teardown();
}

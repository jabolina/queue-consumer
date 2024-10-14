package com.jbolina.base;

import com.jbolina.BenchmarkWorker;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class ThreadQueueWorker implements BenchmarkWorker {

  private static final Consumer<Boolean> consumer = ignore -> {};
  private static final Function<Void, Boolean> TRANSFORM = ignore -> Boolean.TRUE;
  private final BlockingQueueExecutor<Boolean> executor = new BlockingQueueExecutor<>(consumer, 9128);

  @Override
  public CompletableFuture<Boolean> submit() {
    return executor.submit(true)
        .thenApply(TRANSFORM);
  }

  @Override
  public void setup() {
    executor.start();
  }

  @Override
  public void teardown() {
    executor.stop();
  }
}

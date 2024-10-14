package com.jbolina.refactor;

public interface QueueConsumer<T> {

  void consume(Iterable<T> batch);

  int batchSize();

  boolean blocking();
}

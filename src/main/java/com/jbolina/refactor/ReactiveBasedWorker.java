package com.jbolina.refactor;

import com.jbolina.BenchmarkWorker;
import com.jbolina.refactor.impl.BlockingQueueOperationSubmit;
import com.jbolina.refactor.impl.SerializerOperationSubmitter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ReactiveBasedWorker implements BenchmarkWorker {

  private final Consumer<CompletableFuture<Boolean>> consumer = cf -> cf.complete(true);
  private final QueueConsumer<CompletableFuture<Boolean>> qc = new QueueConsumer<>() {
    @Override
    public void consume(Iterable<CompletableFuture<Boolean>> batch) {
      batch.forEach(consumer);
    }

    @Override
    public int batchSize() {
      return 128;
    }

    @Override
    public boolean blocking() {
      return false;
    }
  };
  private final OperationSubmit<CompletableFuture<Boolean>> executor = new SerializerOperationSubmitter<>(new BlockingQueueOperationSubmit<>(qc));

  @Override
  public CompletableFuture<Boolean> submit() {
    CompletableFuture<Boolean> cf = new CompletableFuture<>();
    executor.publish(cf);
    return cf;
  }

  @Override
  public void setup() {
  }

  @Override
  public void teardown() {
  }
}

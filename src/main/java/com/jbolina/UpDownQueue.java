package com.jbolina;

import com.jbolina.refactor.OperationSubmit;
import com.jbolina.refactor.QueueConsumer;
import com.jbolina.refactor.impl.BlockingQueueOperationSubmit;
import com.jbolina.refactor.impl.LeftRightOperationSubmit;
import com.jbolina.refactor.impl.SerializerOperationSubmitter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class UpDownQueue {

  private final OperationSubmit<Request> composed = new LeftRightOperationSubmit<>(
      new BlockingQueueOperationSubmit<>(new RequestConsumer(4096, true)),
      new BlockingQueueOperationSubmit<>(new RequestConsumer(2048, true)),
      Request::isUp
  );
  private final OperationSubmit<Request> queue = new SerializerOperationSubmitter<>(composed);

  public CompletionStage<Void> up() {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    queue.publish(new UpRequest(cf));
    return cf;
  }

  public CompletionStage<Void> down() {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    queue.publish(new DownRequest(cf));
    return cf;
  }

  private interface Request {
    void done();

    boolean isUp();

    default boolean isDown() {
      return !isUp();
    }
  }

  private record UpRequest(CompletableFuture<Void> cf) implements Request {
    @Override
    public void done() {
      cf.complete(null);
    }

    @Override
    public boolean isUp() {
      return true;
    }
  }

  private record DownRequest(CompletableFuture<Void> cf) implements Request {
    @Override
    public void done() {
      cf.complete(null);
    }

    @Override
    public boolean isUp() {
      return false;
    }
  }

  private record RequestConsumer(int batchSize, boolean blocking) implements QueueConsumer<Request> {
    @Override
    public void consume(Iterable<Request> batch) {
      batch.forEach(Request::done);
    }
  }
}

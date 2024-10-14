package com.jbolina.refactor;

public interface OperationSubmit<E> {

  boolean publish(E element);

  QueueConsumer<E> consumer();
}

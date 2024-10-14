package com.jbolina.refactor.impl;

import com.jbolina.refactor.OperationSubmit;
import com.jbolina.refactor.QueueConsumer;
import com.jbolina.refactor.SimpleLinkedArrayList;

import java.util.function.Consumer;

public class SerializerOperationSubmitter<T> implements OperationSubmit<T> {

  private final OperationSubmit<T> submit;
  private final Consumer<Iterable<T>> consumer;
  boolean emitting;
  private SimpleLinkedArrayList<T> queue;

  public SerializerOperationSubmitter(OperationSubmit<T> submit) {
    this.submit = submit;
    this.consumer = submit.consumer()::consume;
  }

  @Override
  public boolean publish(T element) {
    synchronized (this) {
      if (emitting) {
        SimpleLinkedArrayList<T> q = queue;
        if (q == null) {
          queue = q = new SimpleLinkedArrayList<>(4);
        }

        q.add(element);
        return true;
      }
      emitting = true;
    }

    try {
      return submit.publish(element);
    } finally {
      drain();
    }
  }

  @Override
  public QueueConsumer<T> consumer() {
    return submit.consumer();
  }

  private void drain() {
    for (; ; ) {
      SimpleLinkedArrayList<T> q;
      synchronized (this) {
        q = queue;
        if (q == null) {
          emitting = false;
          return;
        }
        queue = null;
      }

      consumer.accept(q);
    }
  }
}

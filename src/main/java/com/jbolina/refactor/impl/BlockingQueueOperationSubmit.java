package com.jbolina.refactor.impl;

import com.jbolina.refactor.OperationSubmit;
import com.jbolina.refactor.QueueConsumer;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class BlockingQueueOperationSubmit<T> extends AtomicLong implements OperationSubmit<T> {

  private final QueueConsumer<T> consumer;
  private final InternalQueue<T> queue;
  private final InternalIter<T> wrapper;
  private final T[] batch;

  public BlockingQueueOperationSubmit(QueueConsumer<T> consumer) {
    this.consumer = consumer;
    this.queue = InternalQueue.getInstance(consumer);
    this.batch = (T[]) new Object[consumer.batchSize()];
    this.wrapper = new InternalIter<T>() {
      private int i = 0;
      private T next;

      @Override
      public void reset() {
        for (int j = 0; j < i; j++) {
          batch[j] = null;
        }
        i = 0;
      }

      @Override
      public Iterator<T> iterator() {
        return this;
      }

      @Override
      public boolean hasNext() {
        if (i >= batch.length) {
          i = 0;
          return false;
        }
        next = batch[i];
        batch[i] = null;
        if (next == null) {
          i = 0;
          return false;
        }

        i++;
        return true;
      }

      @Override
      public T next() {
        return next;
      }
    };
  }

  @Override
  public QueueConsumer<T> consumer() {
    return consumer;
  }

  @Override
  public boolean publish(T element) {
    try {
      return queue.add(element);
    } finally {
      drain();
    }
  }

  private void drain() {
    if (getAndIncrement() != 0)
      return;

    long missed;
    do {
      int processed = 0;
      while (processed != consumer.batchSize()) {
        T curr = queue.poll();
        if (curr == null)
          break;

        batch[processed] = curr;
        processed++;
      }

      missed = addAndGet(-processed);
      consumer.consume(wrapper);
    } while (missed != 0);
  }

  private interface InternalIter<T> extends Iterator<T>, Iterable<T> {
    void reset();
  }

  private interface InternalQueue<E> {
    boolean add(E e);

    E poll();

    static <E> InternalQueue<E> getInstance(QueueConsumer<E> consumer) {
      return consumer.blocking()
          ? new InternalQueue.BlockingQueue<>(consumer.batchSize())
          : new InternalQueue.ResizingQueue<>(consumer.batchSize());
    }

    final class BlockingQueue<E> implements InternalQueue<E> {
      private final ArrayBlockingQueue<E> queue;

      public BlockingQueue(int size) {
        this.queue = new ArrayBlockingQueue<>(size);
      }

      @Override
      public boolean add(E e) {
        try {
          queue.put(e);
          return true;
        } catch (InterruptedException ex) {
          return false;
        }
      }

      @Override
      public E poll() {
        return queue.poll();
      }
    }

    final class ResizingQueue<E> implements InternalQueue<E> {
      private final ArrayDeque<E> queue;

      public ResizingQueue(int size) {
        this.queue = new ArrayDeque<>(size);
      }

      @Override
      public boolean add(E e) {
        return queue.offer(e);
      }

      @Override
      public E poll() {
        return queue.poll();
      }
    }
  }
}

package com.jbolina.refactor.impl;

import com.jbolina.refactor.OperationSubmit;
import com.jbolina.refactor.QueueConsumer;

import java.util.function.Predicate;

public class LeftRightOperationSubmit<E> implements OperationSubmit<E>, QueueConsumer<E> {

  private final OperationSubmit<E> left;
  private final OperationSubmit<E> right;
  private final Predicate<E> predicate;

  public LeftRightOperationSubmit(OperationSubmit<E> left, OperationSubmit<E> right, Predicate<E> predicate) {
    this.left = left;
    this.right = right;
    this.predicate = predicate;
  }

  @Override
  public boolean publish(E element) {
    if (predicate.test(element))
      return left.publish(element);

    return right.publish(element);
  }

  @Override
  public QueueConsumer<E> consumer() {
    return this;
  }

  @Override
  public void consume(Iterable<E> batch) {
    for (E e : batch) {
      publish(e);
    }
  }

  @Override
  public int batchSize() {
    return left.consumer().batchSize() + right.consumer().batchSize();
  }

  @Override
  public boolean blocking() {
    return left.consumer().blocking() || right.consumer().blocking();
  }
}

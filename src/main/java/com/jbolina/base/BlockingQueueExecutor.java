/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jbolina.base;

import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.ThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BlockingQueueExecutor<T> {

  private final Consumer<T> consumer;
  private final BlockingQueue<WorkUnit<T>> processing;
  private final List<WorkUnit<T>> remove;
  private Runner runner;

  public BlockingQueueExecutor(Consumer<T> consumer, int size) {
    this(consumer, size, false);
  }

  public BlockingQueueExecutor(Consumer<T> consumer, int size, boolean start) {
    this.consumer = consumer;
    this.processing = new ArrayBlockingQueue<>(size);
    this.remove = new ArrayList<>();

    if (start) start();
  }

  public void submitSync(T element) {
    WorkUnit<T> wu = new WorkUnit<>(element);
    add(wu);
    wu.join();
  }

  public CompletableFuture<Void> submit(T element) {
    WorkUnit<T> wu = new WorkUnit<>(element);
    add(wu);
    return wu;
  }

  public void stop() {
    runner.stop();
  }

  public void start() {
    ThreadFactory tf = new DefaultThreadFactory("runner", true, true);
    runner = new Runner(tf, "runner", this::processQueue, null);
    runner.start();
  }

  private void add(WorkUnit<T> work) {
    try {
      processing.put(work);
    } catch (InterruptedException e) {
      work.completeExceptionally(e);
    }
  }

  private void processQueue() {
    WorkUnit<T> first;
    try {
      first = processing.poll(1_000L, TimeUnit.MILLISECONDS);
      if (first == null)
        return;

      for (;;) {
        remove.clear();
        if (first != null) {
          remove.add(first);
          first = null;
        }

        processing.drainTo(remove);
        if (remove.isEmpty())
          return;

        for (WorkUnit<T> work : remove) {
          consumer.accept(work.element);
          work.complete(null);
        }
      }
    } catch (InterruptedException ignore) { }
  }

  private static final class WorkUnit<T> extends CompletableFuture<Void> {
    private final T element;

    private WorkUnit(T element) {
      this.element = element;
    }
  }
}

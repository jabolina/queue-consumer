package com.jbolina;

import com.jbolina.refactor.OperationSubmit;
import com.jbolina.refactor.QueueConsumer;
import com.jbolina.refactor.impl.BlockingQueueOperationSubmit;
import com.jbolina.refactor.impl.LeftRightOperationSubmit;
import com.jbolina.refactor.impl.SerializerOperationSubmitter;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

import java.util.function.Predicate;

public class ProposalJCStressTest {

  @State
  @JCStressTest
  @Outcome(id = "3", expect = Expect.ACCEPTABLE, desc = "MPSC")
  public static class NewWorkerTest {
    private final ByRef holder = new ByRef();
    private final QueueConsumer<Integer> consumer = new QueueConsumer<>() {
      @Override
      public void consume(Iterable<Integer> batch) {
        batch.forEach(holder::increment);
      }

      @Override
      public int batchSize() {
        return 64;
      }

      @Override
      public boolean blocking() {
        return false;
      }
    };
    private final OperationSubmit<Integer> executor = new SerializerOperationSubmitter<>(new BlockingQueueOperationSubmit<>(consumer));

    @Actor
    public void actor1() {
      boolean b = executor.publish(1);
      if (!b) throw new IllegalStateException("Element not published");
    }

    @Actor
    public void actor2() {
      boolean b = executor.publish(1);
      if (!b) throw new IllegalStateException("Element not published");
    }

    @Actor
    public void actor3() {
      boolean b = executor.publish(1);
      if (!b) throw new IllegalStateException("Element not published");
    }

    @Arbiter
    public void arbiter(I_Result result) {
      result.r1 = holder.get();
    }
  }

  @State
  @JCStressTest
  @Outcome(id = "4", expect = Expect.ACCEPTABLE, desc = "Composed Submit")
  public static class SingleConsumerMultipleQueue {
    private final ByRef holder = new ByRef();
    private final QueueConsumer<Integer> consumer = new QueueConsumer<>() {
      @Override
      public void consume(Iterable<Integer> batch) {
        batch.forEach(holder::increment);
      }

      @Override
      public int batchSize() {
        return 64;
      }

      @Override
      public boolean blocking() {
        return false;
      }
    };
    private final OperationSubmit<Integer> left = new BlockingQueueOperationSubmit<>(consumer);
    private final OperationSubmit<Integer> right = new BlockingQueueOperationSubmit<>(consumer);
    private final Predicate<Integer> predicate = i -> i % 2 == 0;
    private final OperationSubmit<Integer> composed = new SerializerOperationSubmitter<>(new LeftRightOperationSubmit<>(left, right, predicate));

    @Actor
    public void actor1() {
      composed.publish(1);
    }

    @Actor
    public void actor2() {
      composed.publish(1);
    }

    @Actor
    public void actor3() {
      composed.publish(1);
    }

    @Actor
    public void actor4() {
      composed.publish(1);
    }

    @Arbiter
    public void arbiter(I_Result result) {
      result.r1 = holder.get();
    }
  }

  private static final class ByRef {
    private int value;

    public void increment(int v) {
      value += v;
    }

    public int get() {
      return value;
    }
  }
}

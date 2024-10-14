package com.jbolina.refactor;

import java.util.Iterator;

public class SimpleLinkedArrayList<T> implements Iterable<T> {

  private final int capacity;
  private final Object[] head;
  private Object[] tail;
  private int offset;

  public SimpleLinkedArrayList(int capacity) {
    this.capacity = capacity;
    this.head = new Object[capacity + 1];
    this.tail = head;
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object o) {
    return (T) o;
  }

  public void add(T value) {
    int o = offset;
    if (o == capacity) {
      Object[] next = new Object[capacity + 1];
      tail[capacity] = next;
      tail = next;
      o = 0;
    }
    tail[o] = value;
    offset = o + 1;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<>() {
      private int i = 0;
      private Object[] a = head;
      private T next;

      @Override
      public boolean hasNext() {
        while (a != null) {
          while (i < capacity) {
            Object o = a[i];
            if (o == null)
              break;

            next = cast(o);
            i++;
            return true;
          }
          a = (Object[]) a[capacity];
          i = 0;
        }
        return false;
      }

      @Override
      public T next() {
        return next;
      }
    };
  }
}

package com.worksap.nlp.uzushio.lib.utils;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Iterator;

public final class RowBuffer<T> extends ObjectArrayList<T> {
    public final static class DeletingIterator<T> implements Iterator<T> {
        private final T[] data;
        private final RowBuffer<T> parent;
        private int position;

        public DeletingIterator(RowBuffer<T> parent) {
            this.data = parent.a;
            this.parent = parent;
            this.position = 0;
        }

        @Override
        public boolean hasNext() {
            return position < parent.size;
        }

        @Override
        public T next() {
            T element = data[position];
            position += 1;
            return element;
        }

        public T removeElement() {
            int toRemoveIdx = position - 1;
            T element = parent.removeElementAt(toRemoveIdx);
            position = toRemoveIdx;
            return element;
        }
    }

    public DeletingIterator<T> deletingIterator() {
        return new DeletingIterator<>(this);
    }

    public static <T> RowBuffer<T> single(T x) {
        RowBuffer<T> buffer = new RowBuffer<>();
        buffer.add(x);
        return buffer;
    }

    /**
     * Removes the current element from the collection.
     * Last element is placed instead of the current element.
     * @param index where to remove
     * @return element which replaces current element
     */
    public T removeElementAt(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("index < 0");
        }
        if (index >= size) {
            throw new IllegalArgumentException("index >= size");
        }
        T[] arr = a;
        int lastIdx = size - 1;
        T last = arr[lastIdx];
        arr[lastIdx] = null;
        if (index != lastIdx) {
            arr[index] = last;
        }
        size = lastIdx;
        return last;
    }

    public int addToBuffer(T element) {
        int sz = size;
        add(element);
        return sz;
    }
}

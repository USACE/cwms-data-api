/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.timeseriesprofile;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ParserList extends ArrayList<TimeSeriesProfileParser> {
    private final List<TimeSeriesProfileParser> parsers;

    public ParserList(List<TimeSeriesProfileParser> parsers) {
        this.parsers = parsers;
    }

    public List<TimeSeriesProfileParser> getParsers() {
        return parsers;
    }

    @Override
    public int size() {
        return parsers.size();
    }

    @Override
    public boolean isEmpty() {
        return parsers.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return parsers.contains(o);
    }

    @NotNull
    @Override
    public Iterator<TimeSeriesProfileParser> iterator() {
        return parsers.iterator();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return parsers.toArray(new TimeSeriesProfileParser[0]);
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        return parsers.toArray(a);
    }

    @Override
    public boolean add(TimeSeriesProfileParser e) {
        return parsers.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return parsers.remove(o);
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        return new HashSet<>(parsers).containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesProfileParser> c) {
        return parsers.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends TimeSeriesProfileParser> c) {
        return parsers.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return parsers.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return parsers.retainAll(c);
    }

    @Override
    public void clear() {
        parsers.clear();
    }

    @Override
    public TimeSeriesProfileParser get(int index) {
        return parsers.get(index);
    }

    @Override
    public TimeSeriesProfileParser set(int index, TimeSeriesProfileParser element) {
        return parsers.set(index, element);
    }

    @Override
    public void add(int index, TimeSeriesProfileParser element) {
        parsers.add(index, element);
    }

    @Override

    public TimeSeriesProfileParser remove(int index) {
        return parsers.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return parsers.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return parsers.lastIndexOf(o);
    }

    @NotNull
    @Override
    public ListIterator<TimeSeriesProfileParser> listIterator() {
        return parsers.listIterator();
    }

    @NotNull
    @Override
    public ListIterator<TimeSeriesProfileParser> listIterator(int index) {
        return parsers.listIterator(index);
    }

    @NotNull
    @Override
    public List<TimeSeriesProfileParser> subList(int fromIndex, int toIndex) {
        return parsers.subList(fromIndex, toIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        return parsers.equals(o);
    }

    @Override
    public int hashCode() {
        return parsers.hashCode();
    }

    @Override
    public String toString() {
        return parsers.toString();
    }
}

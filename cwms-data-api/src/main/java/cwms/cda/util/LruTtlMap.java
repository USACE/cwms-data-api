package cwms.cda.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LruTtlMap<K,V> implements Map<K,V> {

    private final long ttl;
    private final int maxSize;
    private final Map<K, TtlEntry<V>> map;

    public LruTtlMap(int maxSize, int ttl)
    {
        this.ttl = ttl;
        this.maxSize = maxSize;
        map = Collections.synchronizedMap(
            new LinkedHashMap<K, TtlEntry<V>>(maxSize, 0.75f, true)
            {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K,LruTtlMap.TtlEntry<V>> eldest) {
                    return this.size() > LruTtlMap.this.maxSize;
                };
            }
        );
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return false; // todo?
    }

    @Override
    public V get(Object key) {
        var entry = map.get(key);
        V ret = null;

        if (entry != null && isExpired(entry)) {
            map.remove(key);
            ret = null;
        } else if (entry != null) {
            ret = entry.value;
        }
        return ret;
    }

    private boolean isExpired(TtlEntry<V> entry)
    {
        return (System.currentTimeMillis() - entry.insertTime) >= ttl;
    }

    @Override
    public V put(K key, V value) {
        return value != null ? map.put(key, new TtlEntry<>(value)).value : null;
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        TtlEntry<V> entry = map.get(key);
        if (entry != null && (isExpired(entry) || entry.value == null)) // treat expired as equivalent to abenst
        {
            map.remove(key);
        }
        return map.computeIfAbsent(key, newKey -> new TtlEntry<V>(mappingFunction.apply(newKey))).value;
    }

    @Override
    public V remove(Object key) {
        var entry = map.remove(key);
        if (entry != null) {
            return entry.value;
        } else {
            return null;
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.forEach((k,v) -> put(k,v));
    }

    @Override
    public void clear() {
        map.clear();;
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values()
                  .stream()
                  .map(e -> e != null ? e.value : null)
                  .collect(Collectors.toSet());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet()
                  .stream()
                  .map(es -> {
                      var v = es.getValue();
                      return new Entry<K,V>() {

                          @Override
                          public K getKey() {
                              return es.getKey();
                          }

                          @Override
                          public V getValue() {
                              return v != null ? v.value : null;
                          }

                          @Override
                          public V setValue(V value) {
                              throw new UnsupportedOperationException(
                                "method 'setValue' is not supported."
                            );
                          }
                      };
                  })
                  .collect(Collectors.toSet());
    }

    private static class TtlEntry<V>
    {
        private final long insertTime;
        private final V value;

        public TtlEntry(V value)
        {
            this.value = value;
            insertTime = System.currentTimeMillis();
        }
    }

}
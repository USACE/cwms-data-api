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

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import hec.data.DataObjectException;
import hec.data.TimeWindow;
import hec.data.TimeWindowMap;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import rma.util.RMAConst;


@JsonDeserialize(builder = WaterSupplyAccounting.Builder.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
public final class WaterSupplyAccounting extends CwmsDTOBase {
    private final String contractName;
    private final WaterUser waterUser;
    private final Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> pumpLocationMap;
    private final Map<CwmsId, TimeWindowMap> pumpTimeWindowMap;

    private WaterSupplyAccounting(Builder builder) {
        this.contractName = builder.contractName;
        this.waterUser = builder.waterUser;
        this.pumpLocationMap = builder.pumpLocationMap;
        this.pumpTimeWindowMap = builder.pumpTimeWindowMap;
    }

    public static class Builder {
        private String contractName;
        private WaterUser waterUser;
        private Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> pumpLocationMap;
        private Map<CwmsId, TimeWindowMap> pumpTimeWindowMap;

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withPumpLocationMap(
                Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> pumpLocationMap) {
            this.pumpLocationMap = pumpLocationMap;
            return this;
        }

        public Builder withPumpTimeWindowMap(Map<CwmsId, TimeWindowMap> pumpTimeWindowMap) {
            this.pumpTimeWindowMap = pumpTimeWindowMap;
            return this;
        }

        public WaterSupplyAccounting build() {
            return new WaterSupplyAccounting(this);
        }

    }

    public String getContractName() {
        return this.contractName;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> getPumpLocationMap() {
        return this.pumpLocationMap;
    }

    public Map<CwmsId, TimeWindowMap> getPumpTimeWindowMap() {
        return this.pumpTimeWindowMap;
    }

    public Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> getAllPumpAccounting() {
        Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> output = new HashMap<>();

        for (Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry
                : this.pumpLocationMap.entrySet()) {
            output.put(cwmsIdNavigableMapEntry.getKey(), new TreeMap<>(cwmsIdNavigableMapEntry.getValue()));
        }
        return output;
    }

    public int size() {
        int size = 0;
        Collection<NavigableMap<Instant, WaterSupplyPumpAccounting>> values = this.pumpLocationMap.values();

        NavigableMap<Instant, WaterSupplyPumpAccounting> value;
        for (Iterator<NavigableMap<Instant, WaterSupplyPumpAccounting>> it = values.iterator();
             it.hasNext(); size += value.size()) {
            value = it.next();
        }
        return size;
    }

    public Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> getAllPumpAccounting(Instant startInstant,
            Instant endInstant) {
        Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> output = new HashMap<>();
        Set<Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>>> entrySet
                = this.pumpLocationMap.entrySet();

        for (Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry : entrySet) {
            NavigableMap<Instant, WaterSupplyPumpAccounting> value = cwmsIdNavigableMapEntry.getValue();
            output.put(cwmsIdNavigableMapEntry.getKey(),
                    value.subMap(startInstant, true, endInstant, true));
        }
        return output;
    }

    public NavigableMap<Instant, WaterSupplyPumpAccounting> getPumpAccounting(CwmsId pumpId) {
        NavigableMap<Instant, WaterSupplyPumpAccounting> map = this.pumpLocationMap.get(pumpId);
        if (map == null) {
            map = this.buildPumpAccounting(pumpId);
        }
        return map;
    }

    public NavigableMap<Instant, WaterSupplyPumpAccounting> getPumpAccounting(CwmsId pumpId, Instant startInstant,
            Instant endInstant) {
        NavigableMap<Instant, WaterSupplyPumpAccounting> map = this.pumpLocationMap.get(pumpId);
        if (map == null) {
            map = this.buildPumpAccounting(pumpId);
            return map;
        }
        return map.subMap(startInstant, true, endInstant, true);
    }

    public NavigableMap<Instant, WaterSupplyPumpAccounting> buildPumpAccounting(CwmsId pumpId) {
        NavigableMap<Instant, WaterSupplyPumpAccounting> accountingMap = this.pumpLocationMap.get(pumpId);
        if (accountingMap != null) {
            return accountingMap;
        } else {
            accountingMap = new TreeMap<>();
            this.pumpLocationMap.put(pumpId, accountingMap);
            return accountingMap;
        }
    }

    public void mergeAccounting(CwmsId pumpLocRef, NavigableMap<Instant, WaterSupplyPumpAccounting> accountingMap,
            boolean generateModifiedTimeWindow, boolean preserveModifiedData) {
        if (pumpLocRef != null && accountingMap != null && !accountingMap.isEmpty()) {
            NavigableMap<Instant, WaterSupplyPumpAccounting> cacheMap
                    = this.buildPumpAccounting(pumpLocRef);
            TimeWindowMap timeWindowMap;
            if (cacheMap != null) {
                this.pumpLocationMap.put(pumpLocRef, new TreeMap<>(accountingMap));
                if (generateModifiedTimeWindow) {
                    timeWindowMap = this.pumpTimeWindowMap.computeIfAbsent(pumpLocRef, k -> new TimeWindowMap());
                    if (!accountingMap.isEmpty()) {
                        try {
                            timeWindowMap.addTimeWindow(new Date(accountingMap.firstKey().toEpochMilli()),
                                true, new Date(accountingMap.lastKey().toEpochMilli()), true);
                        } catch (IllegalArgumentException ex) {
                            Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.SEVERE,
                                    "Start time cannot be after end time", ex);
                        }
                    }
                }
            } else {
                Instant key;
                if (preserveModifiedData) {
                    timeWindowMap = this.getTimeWindowMap(pumpLocRef);
                    if (timeWindowMap != null && !timeWindowMap.isEmpty()) {
                        Set<TimeWindow> timeWindowSet = timeWindowMap.getTimeWindowSet();

                        for (TimeWindow timeWindow : timeWindowSet) {
                            Instant timeWindowStartInstant = timeWindow.getStartDate().toInstant();
                            key = timeWindow.getStartDate().toInstant();
                            SortedMap<Instant, WaterSupplyPumpAccounting> modSet
                                    = accountingMap.subMap(timeWindowStartInstant, true, key, true);
                            if (modSet != null && !modSet.isEmpty()) {
                                Set<Instant> modSetKeys = new HashSet<>(modSet.keySet());
                                accountingMap.keySet().removeAll(modSetKeys);
                            }
                        }
                    }
                }

                NavigableMap<Instant, WaterSupplyPumpAccounting> tempCache = new TreeMap<>(accountingMap);
                tempCache = tempCache.subMap(accountingMap.firstKey(), true,
                        accountingMap.lastKey(), true);
                boolean valuesRemoved = tempCache.keySet().removeAll(accountingMap.keySet());
                if (valuesRemoved) {
                    Set<Map.Entry<Instant, WaterSupplyPumpAccounting>> entrySet = tempCache.entrySet();
                    Iterator<Map.Entry<Instant, WaterSupplyPumpAccounting>> it = entrySet.iterator();

                    breakLabel:
                    while (true) {
                        Map.Entry<Instant, WaterSupplyPumpAccounting> entry;
                        TimeWindowMap pumpTwMap;
                        do {
                            if (!it.hasNext()) {
                                break breakLabel;
                            }

                            entry = it.next();
                            key = entry.getKey();
                            if (!preserveModifiedData) {
                                break;
                            }

                            pumpTwMap = this.getTimeWindowMap(pumpLocRef);
                        } while (pumpTwMap != null
                                && pumpTwMap.containedInTimeWindow(new Date(key.toEpochMilli()), true));

                        WaterSupplyPumpAccounting value = entry.getValue();
                        value.setUndefined();
                    }
                }

                cacheMap.putAll(accountingMap);
                if (generateModifiedTimeWindow) {
                    TimeWindowMap timeWindowMap1 = this.pumpTimeWindowMap.get(pumpLocRef);
                    if (timeWindowMap1 == null) {
                        timeWindowMap1 = new TimeWindowMap();
                        this.pumpTimeWindowMap.put(pumpLocRef, timeWindowMap1);
                    }

                    try {
                        timeWindowMap1.addTimeWindow(new Date(accountingMap.firstKey().toEpochMilli()),
                            true, new Date(accountingMap.lastKey().toEpochMilli()), true);
                    } catch (IllegalArgumentException ex) {
                        Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.SEVERE,
                                "Start time cannot be after end time", ex);
                    }
                }
            }
        }
    }

    public void mergeAccounting(WaterSupplyAccounting waterSupplyAccounting, boolean generateModifiedTimeWindow,
            boolean preserveModifiedData) throws DataObjectException {
        if (!this.getWaterUser().equals(waterSupplyAccounting.getWaterUser())) {
            throw new DataObjectException("Cannot merge accountings for different contracts.");
        } else {
            Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> allPumpAccounting
                    = waterSupplyAccounting.getAllPumpAccounting();
            Set<Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>>> entrySet
                    = allPumpAccounting.entrySet();

            for (Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> entry : entrySet) {
                this.mergeAccounting(entry.getKey(), entry.getValue(),
                        generateModifiedTimeWindow, preserveModifiedData);
            }
        }
    }

    public void removeUndefinedValues() {
        Set<Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>>> entrySet
                = this.pumpLocationMap.entrySet();

        for (Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry : entrySet) {
            NavigableMap<Instant, WaterSupplyPumpAccounting> pumpAccounting = cwmsIdNavigableMapEntry.getValue();
            Set<Map.Entry<Instant, WaterSupplyPumpAccounting>> valueEntrySet = pumpAccounting.entrySet();
            Set<Instant> removeKeys = new HashSet<>();

            for (Map.Entry<Instant, WaterSupplyPumpAccounting> valueEntry : valueEntrySet) {
                WaterSupplyPumpAccounting value = valueEntry.getValue();

                if (!RMAConst.isValidValue(value.getFlow())) {
                    removeKeys.add(valueEntry.getKey());
                }
            }

            pumpAccounting.keySet().removeAll(removeKeys);
        }
    }

    public void clearPumpTimeWindowMaps() {
        Set<Map.Entry<CwmsId, TimeWindowMap>> entrySet = this.pumpTimeWindowMap.entrySet();

        for (Map.Entry<CwmsId, TimeWindowMap> cwmsIdTimeWindowMapEntry : entrySet) {
            (cwmsIdTimeWindowMapEntry.getValue()).clear();
        }
    }

    public TimeWindowMap getTimeWindowMap(CwmsId pumpId) {
        return this.pumpTimeWindowMap.get(pumpId);
    }
}

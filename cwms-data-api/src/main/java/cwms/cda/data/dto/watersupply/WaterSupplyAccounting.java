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

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import hec.data.DataObjectException;
import hec.data.TimeWindow;
import hec.data.TimeWindowMap;
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

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
public class WaterSupplyAccounting extends CwmsDTOBase {
    private final String contractName;
    private final WaterUser waterUser;
    private final Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> pumpLocationMap;
    private final Map<CwmsId, TimeWindowMap> pumpTimeWindowMap;

    public WaterSupplyAccounting(String contractName, WaterUser waterUser,
            Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> pumpLocationMap,
            Map<CwmsId, TimeWindowMap> pumpTimeWindowMap) {
        this.contractName = contractName;
        this.waterUser = waterUser;
        this.pumpLocationMap = pumpLocationMap;
        this.pumpTimeWindowMap = pumpTimeWindowMap;
    }

    public String getContractName() {
        return this.contractName;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> getPumpLocationMap() {
        return this.pumpLocationMap;
    }

    public Map<CwmsId, TimeWindowMap> getPumpTimeWindowMap() {
        return this.pumpTimeWindowMap;
    }

    public Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> getAllPumpAccounting() {
        Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> output = new HashMap<>();

        for (Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry : this.pumpLocationMap.entrySet()) {
            output.put(cwmsIdNavigableMapEntry.getKey(), new TreeMap<>(cwmsIdNavigableMapEntry.getValue()));
        }
        return output;
    }

    public int size() {
        int size = 0;
        Collection<NavigableMap<Date, WaterSupplyPumpAccounting>> values = this.pumpLocationMap.values();

        NavigableMap<Date, WaterSupplyPumpAccounting> value;
        for (Iterator<NavigableMap<Date, WaterSupplyPumpAccounting>> it = values.iterator(); it.hasNext(); size += value.size()) {
            value = it.next();
        }
        return size;
    }

    public Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> getAllPumpAccounting(Date startDate,
            Date endDate) {
        Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> output = new HashMap<>();
        Set<Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>>> entrySet
                = this.pumpLocationMap.entrySet();

        for (Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry : entrySet) {
            NavigableMap<Date, WaterSupplyPumpAccounting> value = cwmsIdNavigableMapEntry.getValue();
            output.put(cwmsIdNavigableMapEntry.getKey(), value.subMap(startDate, true, endDate, true));
        }
        return output;
    }

    public NavigableMap<Date, WaterSupplyPumpAccounting> getPumpAccounting(CwmsId pumpId) {
        NavigableMap<Date, WaterSupplyPumpAccounting> map = this.pumpLocationMap.get(pumpId);
        if (map == null) {
            map = this.buildPumpAccounting(pumpId);
        }
        return map;
    }

    public NavigableMap<Date, WaterSupplyPumpAccounting> getPumpAccounting(CwmsId pumpId, Date startDate,
            Date endDate) {
        NavigableMap<Date, WaterSupplyPumpAccounting> map = this.pumpLocationMap.get(pumpId);
        if (map == null) {
            map = this.buildPumpAccounting(pumpId);
            return map;
        }
        return map.subMap(startDate, true, endDate, true);
    }

    public NavigableMap<Date, WaterSupplyPumpAccounting> buildPumpAccounting(CwmsId pumpId) {
        NavigableMap<Date, WaterSupplyPumpAccounting> accountingMap = this.pumpLocationMap.get(pumpId);
        if (accountingMap != null) {
            return accountingMap;
        } else {
            accountingMap = new TreeMap<>();
            this.pumpLocationMap.put(pumpId, accountingMap);
            return accountingMap;
        }
    }

    public void mergeAccounting(CwmsId pumpLocRef, NavigableMap<Date, WaterSupplyPumpAccounting> accountingMap,
            boolean generateModifiedTimeWindow, boolean preserveModifiedData) {
        if (pumpLocRef != null && accountingMap != null && !accountingMap.isEmpty()) {
            NavigableMap<Date, WaterSupplyPumpAccounting> cacheMap
                    = this.buildPumpAccounting(pumpLocRef);
            TimeWindowMap timeWindowMap;
            if (cacheMap != null) {
                this.pumpLocationMap.put(pumpLocRef, new TreeMap<>(accountingMap));
                if (generateModifiedTimeWindow) {
                    timeWindowMap = this.pumpTimeWindowMap.computeIfAbsent(pumpLocRef, k -> new TimeWindowMap());
                    if (!accountingMap.isEmpty()) {
                        try {
                            timeWindowMap.addTimeWindow(accountingMap.firstKey(), true,
                                    accountingMap.lastKey(), true);
                        } catch (IllegalArgumentException ex) {
                            Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.SEVERE,
                                    "Start time cannot be after end time", ex);
                        }
                    }
                }
            } else {
                Date key;
                if (preserveModifiedData) {
                    timeWindowMap = this.getTimeWindowMap(pumpLocRef);
                    if (timeWindowMap != null && !timeWindowMap.isEmpty()) {
                        Set<TimeWindow> timeWindowSet = timeWindowMap.getTimeWindowSet();

                        for (TimeWindow timeWindow : timeWindowSet) {
                            Date timeWindowStartDate = timeWindow.getStartDate();
                            key = timeWindow.getStartDate();
                            SortedMap<Date, WaterSupplyPumpAccounting> modSet
                                    = accountingMap.subMap(timeWindowStartDate, true, key, true);
                            if (modSet != null && !modSet.isEmpty()) {
                                Set<Date> modSetKeys = new HashSet<>(modSet.keySet());
                                accountingMap.keySet().removeAll(modSetKeys);
                            }
                        }
                    }
                }

                NavigableMap<Date, WaterSupplyPumpAccounting> tempCache = new TreeMap<>(accountingMap);
                tempCache = tempCache.subMap(accountingMap.firstKey(), true,
                        accountingMap.lastKey(), true);
                boolean valuesRemoved = tempCache.keySet().removeAll(accountingMap.keySet());
                if (valuesRemoved) {
                    Set<Map.Entry<Date, WaterSupplyPumpAccounting>> entrySet = tempCache.entrySet();
                    Iterator<Map.Entry<Date, WaterSupplyPumpAccounting>> it = entrySet.iterator();

                    breakLabel:
                    while (true) {
                        Map.Entry<Date, WaterSupplyPumpAccounting> entry;
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
                        } while (pumpTwMap != null && pumpTwMap.containedInTimeWindow(key, true));

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
                        timeWindowMap1.addTimeWindow(accountingMap.firstKey(), true,
                                accountingMap.lastKey(), true);
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
            Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> allPumpAccounting
                    = waterSupplyAccounting.getAllPumpAccounting();
            Set<Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>>> entrySet
                    = allPumpAccounting.entrySet();

            for (Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> entry : entrySet) {
                this.mergeAccounting(entry.getKey(), entry.getValue(),
                        generateModifiedTimeWindow, preserveModifiedData);
            }
        }
    }

    public void removeUndefinedValues() {
        Set<Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>>> entrySet
                = this.pumpLocationMap.entrySet();

        for (Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> cwmsIdNavigableMapEntry : entrySet) {
            NavigableMap<Date, WaterSupplyPumpAccounting> pumpAccounting = cwmsIdNavigableMapEntry.getValue();
            Set<Map.Entry<Date, WaterSupplyPumpAccounting>> valueEntrySet = pumpAccounting.entrySet();
            Set<Date> removeKeys = new HashSet<>();

            for (Map.Entry<Date, WaterSupplyPumpAccounting> valueEntry : valueEntrySet) {
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

    public WaterSupplyAccounting windowAndLimit(Date startTime, Date endTime, boolean headFlag, int rowLimit) {
        WaterSupplyAccounting dst = new WaterSupplyAccounting(this.contractName, this.waterUser, new HashMap<>(),
                new HashMap<>());
        Map<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> allPumpAccounting
                = this.getAllPumpAccounting(startTime, endTime);
        if (allPumpAccounting != null && !allPumpAccounting.isEmpty()) {
            Set<Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>>> entrySet
                    = allPumpAccounting.entrySet();
            Iterator<Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>>> it = entrySet.iterator();

            while (true) {
                NavigableMap<Date, WaterSupplyPumpAccounting> dstPumpAccounting;
                NavigableMap<Date, WaterSupplyPumpAccounting> srcPumpAccounting;
                do {
                    if (!it.hasNext()) {
                        return dst;
                    }

                    Map.Entry<CwmsId, NavigableMap<Date, WaterSupplyPumpAccounting>> entry = it.next();
                    CwmsId pumpId = entry.getKey();
                    dstPumpAccounting = dst.buildPumpAccounting(pumpId);
                    srcPumpAccounting = entry.getValue();
                } while (srcPumpAccounting.isEmpty());

                Object keySet;
                if (headFlag) {
                    keySet = srcPumpAccounting.keySet();
                } else {
                    keySet = srcPumpAccounting.descendingKeySet();
                }

                int num = 0;

                for (Iterator<Set<NavigableMap<Date, WaterSupplyPumpAccounting>>> srcIt
                     = ((Set) keySet).iterator(); srcIt.hasNext(); ++num) {
                    Date key = (Date) srcIt.next();
                    if (num >= rowLimit) {
                        break;
                    }

                    WaterSupplyPumpAccounting srcValue = srcPumpAccounting.get(key);
                    WaterSupplyPumpAccounting srcClone = new WaterSupplyPumpAccounting(srcValue.getWaterUser(),
                            srcValue.getContractName(), srcValue.getPumpLocation(), srcValue.getTransferType(),
                            srcValue.getFlow(), srcValue.getTransferDate(), srcValue.getComment());
                    dstPumpAccounting.put(key, srcClone);
                }

            }
        } else {
            return dst;
        }
    }
}

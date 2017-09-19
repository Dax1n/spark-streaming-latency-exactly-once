package com.daxin;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


public class TimeList implements Serializable {


    private static final long serialVersionUID = 1L;

    // 存储历史数据的容器，权衡读写效率：使用LinkedList提高数据的插入效率（LinkedList的JDK实现是双向链表，查找支持二分查找，所以综合来说LinkedList效率高）
    public List<Long> list = new LinkedList<Long>();

    // 两次时间间隔时间
    private long GAP_TIME_MILLISECOND = 5 * 1000 * 60; //5 * 1000 * 60
    // 保留历史数据时间
    private long RETAIM_TIME_MILLISECOND = 4 * 60 * 1000 * 60;// 保留4小时历史数据，可配置

    // 计数器
    private int count = 0;

    // 检查过期数据的间隔次数
    private int CYCLE_COUNT = 10;

    private long stayTime = 0;
    /**
     * 历史数据的最大时间戳
     */
    private long maxHistoryTime = 0L;

    public long getMaxHistoryTime() {
        return maxHistoryTime;
    }

    public long getStayTime() {
        return stayTime;
    }

    /**
     * 添加当前批次的停留时间
     *
     * @param batchStayTime
     */
    public void addCurrentBatchStayTime(long batchStayTime) {
        stayTime = stayTime + batchStayTime;
    }

    /**
     * 添加由于记录迟到的补偿时间
     *
     * @param compensateTime
     */
    private void addCompensateTime(long compensateTime) {
        stayTime = stayTime + compensateTime;
    }

    /**
     * 有序进行二分查找插入位置 <br>
     * 如果集合中存在的话返回-1，否则返回插入的位置
     *
     * @param e
     * @return 返回插入位置
     */
    private int binaryInsert(Long e) {
        if (list.contains(e))
            return -1;
        return Math.abs(Collections.binarySearch(list, e)) - 1;
    }

    /**
     * 添加当前批次数据，没有时间错
     *
     * @param es
     * @return
     */
    public void addElementsWithOutTime(long[] es) {
        if (es.length == 0)
            return;

        for (int i = 0; i < es.length; i++) {
            addElementWithoutTime(es[i]);// 优化二分查找思路进行插入
        }

    }

    /**
     * 添加当前批次的数据到历史数据中，此部分数据没有时间补偿
     *
     * @param e
     * @return
     */
    private void addElementWithoutTime(Long e) {

        int pos = binaryInsert(e);

        if (pos == -1)// 返回位置-1的话,元素已经存在无需插入
            return;

        list.add(pos, e);
        maxHistoryTime = list.size() == 0 ? 0 : Collections.max(list);

        // 补偿数据记录插入历史数据列表中
        // 每10条记录更新一下历史数据。以后还可以添加时间条件更新历史数据
        if ((++count) % CYCLE_COUNT == 0) {
            removeExpireData();
        }

    }

    /**
     * 移除过期数据，过期时间为：RETAIM_TIME_MILLISECOND
     * <p/>
     * 目前是根据每添加10条历史数据检查一下过期数据，后期还可以设置间隔时间进行更新过期数据。（模拟Redis的机制）
     */
    private void removeExpireData() {

        Long maxTime = Collections.max(list);

        for (int i = 0; i < list.size(); i++) {
            if (maxTime - list.get(i) > RETAIM_TIME_MILLISECOND) {
                list.remove(i);
                i--;
            } else {
                break;
            }
        }
        count = 0;

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("最大历史时间：").append(maxHistoryTime).append(", 累计停留时间：").append(stayTime);

        return sb.toString();
    }

    // ---------------------------------------------------------

    /**
     * 计算补偿时间,需要判断满足补偿条件才可以调用
     *
     * @param list
     * @param insertIndex
     * @return
     */
    public long computeCompensateTime(List<Long> list, int insertIndex) {

        if (insertIndex == 0) { // 插入第一个位置
            long gap = list.get(1) - list.get(0);
            return gap < GAP_TIME_MILLISECOND ? gap : 0;
        } else if (insertIndex + 1 == list.size()) { // 插入的位置是后一个位置
            long gap = (list.get(insertIndex + 1) - list.get(insertIndex));
            return gap < GAP_TIME_MILLISECOND ? gap : 0;
        } else {
            long gap1 = list.get(insertIndex) - list.get(insertIndex - 1);
            long gap2 = list.get(insertIndex + 1) - list.get(insertIndex);
            long time1 = gap1 < GAP_TIME_MILLISECOND ? gap1 : 0L;
            long time2 = gap2 < GAP_TIME_MILLISECOND ? gap2 : 0L;
            return time1 + time2;
        }
    }

    /**
     * @param es
     * @return 返回es对应的每一个补偿时间数组
     */
    public long[] addElements(long[] es) {
        if (es.length == 0)
            return new long[0];
        long[] result = new long[es.length];
        for (int i = 0; i < es.length; i++) {
            result[i] = addElement(es[i]);// 优化二分查找思路进行插入
        }
        // 更新最大历史时间

        return result;

    }

    /**
     * 返回停留时间
     *
     * @param e 时间单位是毫秒
     * @return 返回时间单位是毫秒
     */
    private long addElement(Long e) {

        int pos = binaryInsert(e);

        if (pos == -1)// 已经存在
            return 0L;

        if (pos == 0) {// 插入位置是的第一个位置
            // 1：集合为空，
            if (list.size() == 0) {
                // 没有补偿时间，更新一下历史最大时间即可
                list.add(pos, e);
                maxHistoryTime = e;
                return 0L;
            } else {
                // 2：集合不为空,插入位置是0位置，无需更新最大历史时间
                list.add(pos, e);
                if (list.get(pos + 1) - list.get(pos) < GAP_TIME_MILLISECOND) {// 两个记录间隔小于5分钟，满足补偿条件
                    long compensateTime = computeCompensateTime(list, pos);
                    addCompensateTime(compensateTime);
                    return compensateTime;
                } else {
                    return 0L;
                }
            }

        }

        // 插入在集合的最后一个位置
        if (pos == list.size()) {
            list.add(pos, e);
            // 插入在最后一个位置，所以e就是最大时间戳
            maxHistoryTime = e;
            if (list.get(pos + 1) - list.get(pos) < GAP_TIME_MILLISECOND) { // 补偿条件
                long compensateTime = computeCompensateTime(list, pos);
                addCompensateTime(compensateTime);
                // 更新最大历史时间

                return compensateTime;
            } else { // 没有补偿条件
                return 0L;
            }

        }

        // 插在中间的位置,只要判断一下是否满足补偿条件即可，无需更新最大历史时间
        list.add(pos, e);

        if (list.get(pos + 1) - list.get(pos - 1) >= GAP_TIME_MILLISECOND) {
            // 满足补偿条件
            long compensateTime = computeCompensateTime(list, pos);
            addCompensateTime(compensateTime);
            return compensateTime;
        }

        return 0L;
    }

}

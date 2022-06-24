package com.four5prings.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.four5prings.constants.GmallConstants;
import com.four5prings.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @ClassName CanalClient
 * @Description
 * @Author Four5prings
 * @Date 2022/6/24 11:12
 * @Version 1.0
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        /**
         * SocketAddress address, String destination, String username,String password
         * 搜索出canalconnector连接器接口，找到实现类调用创建连接方法创建连接器，再使用连接器创建连接
         */
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        //创建连接，为了保证方法一直运行，可以使用while循环让main线程一直运行
        while (true) {
            //创建连接
            canalConnector.connect();

            //订阅数据库
            canalConnector.subscribe("gmall2022.*");

            //通过连接获取message
            Message message = canalConnector.get(100);

            /**
             * 获取到message后，我们需要进行指标的判断，这里是GMV的指标，所以我们只需要order_info表的信息,
             * message 内部含有多个sql执行的结果，一个sql对应的结果就是一个entry
             */
            List<CanalEntry.Entry> entries = message.getEntries();
            //先进行健壮性判断，如果没有数据，那么就线程等待
            if(entries.size()<=0){
                //说明没有数据，线程等待
                System.out.println("there is no data,please waitting for 5seconds");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }else {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();
                    //获取EntryType类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //判断entrytype是不是rowdata
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //TODO 根据条件获取数据
                        handle(tableName,eventType,rowDatasList);
                    }
                }
            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表的新增数据
        if("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放所有列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                //使用json转换
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());

            }
        }
    }
}

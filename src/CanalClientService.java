package com.huafa.core.configuration;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

/**
 * @ClassName: CanalClientService
 * @Description: TODO
 * @Author wangpeng
 * @Date 2019-6-24 17:59
 * @Version 1.0
 */
@Component
public class CanalClientService implements DisposableBean, ApplicationListener<ContextRefreshedEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClientService.class);

    private CanalConfig canalConfig;

    //定时任务检查 canal连接状态
    private ScheduledExecutorService scheduledExecutorService;

    //canal线程处理，不阻塞springboot主流程
    private ExecutorService canalExecutor;

    private static volatile boolean start;

    private CanalConnector connector;

    //需要监听过滤的一些表
    private static Set<String> tableNames = new HashSet<>();

    static
    {
        //初始化一些需要监听的表
        tableNames.add("contract_collection_plan");
    }

    @Autowired
    public CanalClientService(CanalConfig canalConfig)
    {
        this.canalConfig = canalConfig;
    }

    @Override
    public void destroy() throws Exception
    {
        LOGGER.error(">>> 即将关闭Canal连接，销毁线程池.....");
        if (start)
        {
            try
            {
                connector.disconnect();
            }
            catch (CanalClientException e)
            {
                LOGGER.error(">>> 关闭Canal连接异常：", e);
            }
        }
        canalExecutor.shutdown();
        scheduledExecutorService.shutdown();
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent)
    {

        LOGGER.info(">>> Canal启动......");
        init();
        // 执行启动
        canalExecutor.execute(() -> start());
        // 启动后5S开始检查connector的状态，每个2两分钟执行一次
        scheduledExecutorService.scheduleAtFixedRate(() -> check(), 1, 2, TimeUnit.MINUTES);

    }

    private void start()
    {
        try
        {
            connector.connect();
            connector.subscribe(canalConfig.getListenerDb() + "\\..*");
            start = true;
            LOGGER.info(">>> Canal连接成功，订阅DB：{}下所有表信息", canalConfig.getListenerDb());
        }
        catch (CanalClientException e)
        {
            LOGGER.error(">>> Canal服务连接失败：", e);
            start = false;
        }

        if (start)
        {
            processBinlog();
        }
    }

    private void processBinlog()
    {
        //每一次拉取100条数据
        int batchSize = 100;
        Message msg;
        while (true)
        {
            try
            {
                msg = connector.getWithoutAck(batchSize);
                long batchId = msg.getId();
                int size = msg.getEntries().size();
                //没有数据，休眠5秒
                if (batchId < 0 || size == 0)
                {
                    try
                    {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException e)
                    {
                        LOGGER.error(">>> 休眠线程中断.......");
                    }
                }
                else
                {
                    // 数据处理
                    processData(msg.getEntries());
                }
                connector.ack(batchId);
            }
            catch (Exception e)
            {
                // 客户端异常，中断当前循环，等待定时任务重连操作
                LOGGER.error(">>> Canal 客户端异常，中断操作，并断开现有连接，等待定时任务定期重连操作...", e);
                this.close();
                break;
            }
        }
    }

    private void processData(List<CanalEntry.Entry> entrys)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("接收到需要处理数据，总数量：{}", entrys.size());
        }

        //记录实际更新记录数
        int handleNum = 0;

        // 表名
        String tableName;
        CanalEntry.RowChange rowChange;
        for (CanalEntry.Entry entry : entrys)
        {
            // 事物数据不处理
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND)
            {
                continue;
            }

            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA)
            {
                continue;
            }

            //获取表名
            tableName = entry.getHeader().getTableName();

            //仅处理部分表接口数据
            if (tableNames.contains(tableName))
            {
                try
                {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    // RowData --具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理
                    // insert 只有after数据，delete只有 before数据，update会有after和before数据
                    for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                    {
                        switch (rowChange.getEventType())
                        {
                            //当前仅处理Insert和更新数据
                            case INSERT:
                            case UPDATE:
                                printColumn(rowData.getAfterColumnsList());
                                handleNum ++ ;
                                break;
                            default:
                                break;
                        }
                    }
                }
                catch (Exception e)
                {
                    LOGGER.error("当前行数据解析错误：", e);
                }
            }
        }
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("实际更新总数量：{}", handleNum);
        }
    }

    private void printColumn( List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    /**
     * 解析主键KEY<br>
     * CanalEntry.Column字段：<br>
     * <p>
     * sqlType     [jdbc type]
     * <p>
     * name        [字段名]
     * <p>
     * isKey       [是否为主键]
     * <p>
     * updated     [是否发生过变更]
     * <p>
     * isNull      [值是否为null]
     * <p>
     * value       [具体的内容，注意为string文本]
     *
     * @param columns
     */
    private Long parseKey(List<CanalEntry.Column> columns)
    {
        // 获取操作后完整的整条记录
        String line = columns.stream().map(column -> column.getName() + "=" + column.getValue())
                .collect(Collectors.joining(","));
        //只保留主键Key的数据
        Stream<CanalEntry.Column> columnStream = columns.stream().filter(column -> column.getIsKey());
        // 返回第一个数据
        Optional<CanalEntry.Column> first = columnStream.findFirst();
        return Long.parseLong(first.get().getValue());
    }

    private void check()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(">>> 开始执行检测Canal连接状态...");
        }
        if (!start)
        {
            LOGGER.warn(">>> 开始执行重连Canal服务...");
            this.start();
        }
    }

    private void init()
    {
        LOGGER.info(">>> 初始化Canal连接信息.......");
        int threads = Runtime.getRuntime().availableProcessors();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(threads);

        canalExecutor = Executors.newSingleThreadExecutor();

        LOGGER.info(">>> 建立单Canal连接信息：{}", canalConfig.getHostName());
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getHostName(), canalConfig
                .getPort()), canalConfig.getDestination(), canalConfig.getUserName(), canalConfig.getPassword());
    }

    private void close()
    {
        try
        {
            if (start)
            {
                connector.disconnect();
            }
        }
        catch (CanalClientException e)
        {
            LOGGER.error("关闭Canal连接错误：", e);
        }
        finally
        {
            start = false;
        }

    }

}

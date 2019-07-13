
# 背景
近期接到一个任务，需要改造现有从mysql往Elasticsearch导入数据MTE(mysqlToEs)小工具，由于之前采用单线程导入，千亿数据需要两周左右的时间才能导入完成，导入效率非常低。所以楼主花了3天的时间，利用java线程池框架Executors中的FixedThreadPool线程池重写了MTE导入工具，单台服务器导入效率提高十几倍（合理调整线程数据，效率更高）。

# 关键技术栈
- Elasticsearch
- jdbc
- ExecutorService\Thread
- sql

# 工具说明
**maven依赖**
```
<dependency>
 <groupId>mysql</groupId>
 <artifactId>mysql-connector-java</artifactId>
 <version>${mysql.version}</version>
</dependency>
<dependency>
 <groupId>org.elasticsearch</groupId>
 <artifactId>elasticsearch</artifactId>
 <version>${elasticsearch.version}</version>
</dependency>
<dependency>
 <groupId>org.elasticsearch.client</groupId>
 <artifactId>transport</artifactId>
 <version>${elasticsearch.version}</version>
</dependency>
<dependency>
 <groupId>org.projectlombok</groupId>
 <artifactId>lombok</artifactId>
 <version>${lombok.version}</version>
</dependency>
<dependency>
 <groupId>com.alibaba</groupId>
 <artifactId>fastjson</artifactId>
 <version>${fastjson.version}</version>
</dependency>
```

**java线程池设置**

默认线程池大小为21个，可调整。其中POR为处理流程已办数据线程池，ROR为处理流程已阅数据线程池。

```
private static int THREADS = 21;
public static ExecutorService POR = Executors.newFixedThreadPool(THREADS);
public static ExecutorService ROR = Executors.newFixedThreadPool(THREADS);
```

**定义已办生产者线程/已阅生产者线程：ZlPendProducer/ZlReadProducer**

```
public class ZlPendProducer implements Runnable {
 ...
 @Override
 public void run() {
 System.out.println(threadName + "::启动...");
 for (int j = 0; j < Const.TBL.TBL_PEND_COUNT; j++)
 try {
 ....
 int size = 1000;
 for (int i = 0; i < count; i += size) {
 if (i + size > count) {
 //作用为size最后没有100条数据则剩余几条newList中就装几条
 size = count - i;
 }
 String sql = "select * from " + tableName + " limit " + i + ", " + size;
 System.out.println(tableName + "::sql::" + sql);
 rs = statement.executeQuery(sql);
 List<HistPendingEntity> lst = new ArrayList<>();
 while (rs.next()) {
 HistPendingEntity p = PendUtils.getHistPendingEntity(rs);
 lst.add(p);
 }
 MteExecutor.POR.submit(new ZlPendConsumer(lst));
 Thread.sleep(2000);
 }
 ....
 } catch (Exception e) {
 e.printStackTrace();
 }
 }
}
public class ZlReadProducer implements Runnable {
 ...已阅生产者处理逻辑同已办生产者
}
```
**定义已办消费者线程/已阅生产者线程：ZlPendConsumer/ZlReadConsumer**
```
public class ZlPendConsumer implements Runnable {
 private String threadName;
 private List<HistPendingEntity> lst;
 public ZlPendConsumer(List<HistPendingEntity> lst) {
 this.lst = lst;
 }
 @Override
 public void run() {
 ...
 lst.forEach(v -> {
 try {
 String json = new Gson().toJson(v);
 EsClient.addDataInJSON(json, Const.ES.HistPendDB_Index, Const.ES.HistPendDB_type, v.getPendingId(), null);
 Const.COUNTER.LD_P.incrementAndGet();
 } catch (Exception e) {
 e.printStackTrace();
 System.out.println("err::PendingId::" + v.getPendingId());
 }
 });
 ...
 }
}
public class ZlReadConsumer implements Runnable {
 //已阅消费者处理逻辑同已办消费者
}
```
**定义导入Elasticsearch数据监控线程：Monitor**

监控线程-Monitor为了计算每分钟导入Elasticsearch的数据总条数，利用监控线程，可以调整线程池的线程数的大小，以便利用多线程更快速的导入数据。
```
public void monitorToES() {
 new Thread(() -> {
 while (true) {
 StringBuilder sb = new StringBuilder();
 sb.append("已办表数::").append(Const.TBL.TBL_PEND_COUNT)
 .append("::已办总数::").append(Const.COUNTER.LD_P_TOTAL)
 .append("::已办入库总数::").append(Const.COUNTER.LD_P);
 sb.append("~~~~已阅表数::").append(Const.TBL.TBL_READ_COUNT);
 sb.append("::已阅总数::").append(Const.COUNTER.LD_R_TOTAL)
 .append("::已阅入库总数::").append(Const.COUNTER.LD_R);
 if (ldPrevPendCount == 0 && ldPrevReadCount == 0) {
 ldPrevPendCount = Const.COUNTER.LD_P.get();
 ldPrevReadCount = Const.COUNTER.LD_R.get();
 start = System.currentTimeMillis();
 } else {
 long end = System.currentTimeMillis();
 if ((end - start) / 1000 >= 60) {
 start = end;
 sb.append("\n#########################################\n");
 sb.append("已办每分钟TPS::" + (Const.COUNTER.LD_P.get() - ldPrevPendCount) + "条");
 sb.append("::已阅每分钟TPS::" + (Const.COUNTER.LD_R.get() - ldPrevReadCount) + "条");
 ldPrevPendCount = Const.COUNTER.LD_P.get();
 ldPrevReadCount = Const.COUNTER.LD_R.get();
 }
 }
 System.out.println(sb.toString());
 try {
 Thread.sleep(3000);
 } catch (InterruptedException e) {
 e.printStackTrace();
 }
 }
 }).start();
}
```
**初始化Elasticsearch：EsClient**
```
String cName = meta.get("cName");//es集群名字
String esNodes = meta.get("esNodes");//es集群ip节点
Settings esSetting = Settings.builder()
 .put("cluster.name", cName)
 .put("client.transport.sniff", true)//增加嗅探机制，找到ES集群
 .put("thread_pool.search.size", 5)//增加线程池个数，暂时设为5
 .build();
String[] nodes = esNodes.split(",");
client = new PreBuiltTransportClient(esSetting);
for (String node : nodes) {
 if (node.length() > 0) {
 String[] hostPort = node.split(":");
 client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.parseInt(hostPort[1])));
 }
}
```
**初始化数据库连接**
```
conn = DriverManager.getConnection(url, user, password);
```
**启动参数**
```
nohup java -jar mte.jar ES-Cluster2019 192.168.1.10:9300,192.168.1.11:9300,192.168.1.12:9300 root 123456! jdbc:mysql://192.168.1.13
:3306/mte 130 130 >> ./mte.log 2>&1 &
```
参数说明

ES-Cluster2019 为Elasticsearch集群名字
192.168.1.10:9300,192.168.1.11:9300,192.168.1.12:9300为es的节点IP
130 130为已办已阅分表的数据

**程序入口：MteMain**
```
// 监控线程
Monitor monitorService = new Monitor();
monitorService.monitorToES();
// 已办生产者线程
Thread pendProducerThread = new Thread(new ZlPendProducer(conn, "ZlPendProducer"));
pendProducerThread.start();
// 已阅生产者线程
Thread readProducerThread = new Thread(new ZlReadProducer(conn, "ZlReadProducer"));
readProducerThread.start();
```

Storm connector for HBase using AsyncHBase client
=================================================

This connector for Apache Storm use AsyncHBase client to 
persist raw data and  Trident states to Apache HBase.
 
Benefits
--------

AyncHBase client a is fully asynchronous and thread-safe client
for Apache HBase. Unlike the traditional HBase client (HTable)
you only need one instance of the client per HBase cluster you
want to interact with. Even if you want to interact with
multiple tables. It avoid unnecessary waiting threads and
allow batch of requests to run in parallel even in synchronous
mode.  
The client provide client side buffering, which may induce
some latency in your topology. You might tweak the default 100ms
flush interval if you care about latency. This connector
tries to provide flexibility and high performance, the non-blocking
fashion should reduce pressure on both Storm and HBase.
I suggest you to read the javadoc for all more detailed information.

Usage
-----

Usage example can be found in the storm.asynchbase.example package

 * Client configuration  
You have to register a configuration Map in the topology Config for
each client you want to use. 

```
    Map<String, String> hBaseConfig = new HashMap<>();
    hBaseConfig.put("zkQuorum", "node1,node2,node3");
    hBaseConfig.put("zkPath", "/hbase");
    conf.put("hbase-cluster", hBaseConfig);
```        

 * Mapper  
To map Storm tuple to HBase RPC requests you'll have to provide
some mappers to the bolts, function, states.
You'll be able to use method chaining syntax to configure them.
You can either map a query parameter to a tuple field or to a
fixed constant value. You can also provide serializers to
format input values to a specific type.

```
    IAsyncHBaseMapper mapper = new AsyncHBaseMapper()
                .addFieldMapper(new AsyncHBaseFieldMapper()
                        .setTable("test")
                        .setRowKeyField("key")
                        .setColumnFamily("data")
                        .setColumnQualifierField("value")
                        .setColumnQualifierSerializer(new IntegerSerializer())
                        .setValue(payload)
                );
```

 * Bolts  
AsyncHBaseBolt is used to execute requests for each incoming tuple, it use
on or more FieldMapper to build the requests from the tuple's fields. All 
requests executed from a tuple are executed in parallel. By default this 
bolt is asynchronous, be sure to read the doc to fully understand what it
does.

```
    builder.setBolt(
            "hbase-bolt",
            new AsyncHBaseBolt("hbase-cluster", mapper),
            5).noneGrouping("spout");
```

ExtractKeyValuesBolt is used to extract cells values returned by a GetRequests
to some tuples containing the cell property as field values. You can choose
which property you want to retrieve with the boolean flags passed to the
constructor. You can also provide deserializer to map property to a 
specific type.

```
    builder.setBolt("extract-bolt",
        new ExtractKeyValues(false, false, false, true, false) // return only the value
        .setValueDeserializer(new IntSerializer())
```

 * Trident Operations  
ExecuteHBaseRpcs is a Trident function that mimic the behaviour of the
ExtractKeyValuesBolt in a trident topology.

```
    .each(new Fields("args"), new ExecuteHBaseRpcs("hbase-cluster", mapper, "get values").setAsync(false), new Fields("values"))    
```

ExtractKeyValues is a Trident function that mimic the behaviour of the
AsyncHBaseBolt in a trident topology.

```
    .each(
        new Fields("values"),
        new ExtractKeyValues(false, false, false, true, false)
            .setValueDeserializer(new IntSerializer()),
        new Fields("value"))
```

 * Trident State  
This is a TridentState implementation to persist a partition to HBase using AsyncHBase client.
It should be used with the partition persist 
You should only use this state if your update is idempotent regarding batch replay. Use the
AsyncHBaseStateUpdater / AsyncHBaseStateQuery and AsyncHBaseStateFactory to interact with it.
You have to provide mappers for update and(or) query function.

```
    AsyncHBaseState.Options streamRateOptions = new AsyncHBaseState.Options();
    streamRateOptions.cluster = "hbase-cluster";
    
    streamRateOptions.updateMapper = new AsyncHBaseTridentFieldMapper()
         .setTable("test")
         .setColumnFamily("data")
         .setColumnQualifier("stream rate")
         .setRowKey("global rate")
         .setValueField("rate")
         .setValueSerializer(new AsyncHBaseLongSerializer());
         
    streamRateOptions.queryMapper = new AsyncHBaseTridentFieldMapper()
         .setRpcType(IAsyncHBaseTridentFieldMapper.Type.GET)
         .setTable("test")
         .setColumnFamily("data")
         .setColumnQualifier("stream rate")
         .setRowKey("global rate");
    
    TridentState streamRate = stream
            .aggregate(new Fields(), new StreamRateAggregator(2), new Fields("rate"))
            .partitionPersist(
                new AsyncHBaseStateFactory(streamRateOptions),
                new Fields("rate"),
                new AsyncHBaseStateUpdater()
            );
    
    topology.newDRPCStream("stream rate drpc", drpc)
        .stateQuery(
            streamRate,
            new AsyncHBaseStateQuery()
                .setValueDeserializer(new AsyncHBaseLongSerializer()),
            new Fields("rate"))
```

 * Trident MapState
This is a Trident State implementation backed by HBase using AsyncHBase client.
It can provide an LRU Cache to speed up multiget. It can handle both 
NON-TRANSACTIONAL, TRANSACTIONAL and OPAQUE modes.

```
    AsyncHBaseMapState.Options sumStateOptions = new AsyncHBaseMapState.Options<TransactionalValue>();
        sumStateOptions.cluster = "hbase-cluster";
        sumStateOptions.table = "test";
        sumStateOptions.columnFamily = "data";
        sumStateOptions.columnQualifier = "total";
        
    TridentState sumState = stream
                .groupBy(new Fields("key"))
                .persistentAggregate(
                    AsyncHBaseMapState.transactional(sumStateOptions),
                    new Fields("value"),
                    new Sum(),
                    new Fields("sum"))
                    .parallelismHint(10);
```
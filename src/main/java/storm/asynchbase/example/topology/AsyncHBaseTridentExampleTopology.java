/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.example.spout.RandomKeyValueBatchSpout;
import storm.asynchbase.example.trident.operation.AverageAggregator;
import storm.asynchbase.trident.mapper.AsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.AsyncHBaseTridentMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.trident.operation.ExecuteHBaseRpcs;
import storm.asynchbase.trident.operation.ExtractKeyValues;
import storm.asynchbase.utils.AsyncHBaseDeserializer;
import storm.asynchbase.utils.AsyncHBaseSerializer;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This topology use Trident to insert random values in random columns of an HBase row.<br/>
 * Then it use DRPC to calculate the average value of the row, then it wipes the row.<br/>
 * There is also a DRPC counter using AtomicIncrement.<br/>
 * </p>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class AsyncHBaseTridentExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseTridentExampleTopology.class);

    public static StormTopology buildTopology(LocalDRPC drpc) {

        class IntSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer {
            @Override
            public byte[] serialize(Object object) {
                return Integer.toString((int) object).getBytes();
            }

            @Override
            public Object deserialize(byte[] value) {
                return Integer.parseInt(new String(value));
            }

            @Override
            public void prepare(Map conf) {
            }
        }

        /**
         * Topology
         */

        TridentTopology topology = new TridentTopology();

        IAsyncHBaseTridentMapper mapper = new AsyncHBaseTridentMapper()
            // Insert a random value in a random column of a fixed row.
            .addFieldMapper("save", new AsyncHBaseTridentFieldMapper()
                .setTable("test")
                .setColumnFamily("data")
                .setColumnQualifierField("key")
                .setRowKey("key")
                .setValueField("value")
                .setValueSerializer(new IntSerializer())
                .setBufferable(false))

                // Get all value of the row.
            .addFieldMapper("get values", new AsyncHBaseTridentFieldMapper()
                .setRpcType(IAsyncHBaseTridentFieldMapper.Type.GET)
                .setTable("test")
                .setRowKey("key"))

                // Wipe the row.
            .addFieldMapper("clean", new AsyncHBaseTridentFieldMapper()
                .setRpcType(IAsyncHBaseTridentFieldMapper.Type.DELETE)
                .setTable("test")
                .setColumnFamily("data")
                .setRowKey("key"))

                // Increase drpc counter.
            .addFieldMapper("incr", new AsyncHBaseTridentFieldMapper()
                    .setRpcType(IAsyncHBaseTridentFieldMapper.Type.INCR)
                    .setTable("test")
                    .setColumnFamily("data")
                    .setColumnQualifier("drpc-counter")
                    .setRowKey("counters")
                    .setIncrement(1)
            );

        Stream stream = topology.newStream("stream", new RandomKeyValueBatchSpout(10).setSleep(1000)).parallelismHint(5);

        stream
            .each(new Fields("key", "value"), new ExecuteHBaseRpcs("hbase-cluster", mapper, "save"), new Fields("")).parallelismHint(10)
            .each(new Fields("key", "value"), new Debug());

        ArrayList<String> drpcRequests = new ArrayList<>();
        drpcRequests.add("clean");
        drpcRequests.add("incr");


        topology.newDRPCStream("average-drpc", drpc)
            .each(new Fields("args"), new ExecuteHBaseRpcs("hbase-cluster", mapper, "get values").setAsync(false), new Fields("values"))
            .each(
                new Fields("values"),
                new ExtractKeyValues(false, false, false, true, false)
                    .setValueDeserializer(new IntSerializer()),
                new Fields("value"))
            .aggregate(new Fields("value"), new AverageAggregator(), new Fields("average"))
            .each(new Fields("average"), new Debug())
            .each(new Fields(), new ExecuteHBaseRpcs("hbase-cluster", mapper, drpcRequests), new Fields(""));
        ;

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        Map<String, String> hBaseConfig = new HashMap<>();
        hBaseConfig.put("zkQuorum", "node-00113.hadoop.ovh.net,node-00114.hadoop.ovh.net,node-00116.hadoop.ovh.net");
        conf.put("hbase-cluster", hBaseConfig);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(5);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        } else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HBaseTridentExampleTopology", conf, buildTopology(drpc));
            while (true) {
                log.info(drpc.execute("average-drpc", ""));
                Thread.sleep(10000);
            }
        }
    }
}
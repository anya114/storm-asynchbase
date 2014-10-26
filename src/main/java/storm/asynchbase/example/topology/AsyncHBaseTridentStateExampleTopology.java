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
import storm.asynchbase.example.trident.operation.StreamRateAggregator;
import storm.asynchbase.trident.mapper.AsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.AsyncHBaseTridentMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.trident.state.AsyncHBaseState;
import storm.asynchbase.trident.state.AsyncHBaseStateFactory;
import storm.asynchbase.trident.state.AsyncHBaseStateQuery;
import storm.asynchbase.trident.state.AsyncHBaseStateUpdater;
import storm.asynchbase.utils.serializer.AsyncHBaseDeserializer;
import storm.asynchbase.utils.serializer.AsyncHBaseSerializer;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This topology use Trident to compute the average stream rate over the last 2 seconds.<br/>
 * And then persisit it to HBase using AsyncHBaseState.<br/>
 * A DRPC query the state every second.<br/>
 * </p>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class AsyncHBaseTridentStateExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseTridentStateExampleTopology.class);

    public static StormTopology buildTopology(LocalDRPC drpc) {

        class AsyncHBaseLongSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer {
            @Override
            public byte[] serialize(Object value) {
                return Long.toString((long) value).getBytes();
            }

            @Override
            public Object deserialize(byte[] value) {
                return Long.parseLong(new String(value));
            }

            @Override
            public void prepare(Map conf) {
            }
        }

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("stream", new RandomKeyValueBatchSpout(10).setSleep(100)).parallelismHint(5);

        IAsyncHBaseTridentMapper updateMapper = new AsyncHBaseTridentMapper()
            .addFieldMapper( new AsyncHBaseTridentFieldMapper()
                .setTable("test")
                .setColumnFamily("data")
                .setColumnQualifier("stream rate")
                .setRowKey("global rate")
                .setValueField("rate")
                .setValueSerializer(new AsyncHBaseLongSerializer())
            );

        IAsyncHBaseTridentMapper queryMapper = new AsyncHBaseTridentMapper()
            .addFieldMapper( new AsyncHBaseTridentFieldMapper()
                .setRpcType(IAsyncHBaseTridentFieldMapper.Type.GET)
                .setTable("test")
                .setColumnFamily("data")
                .setColumnQualifier("stream rate")
                .setRowKey("global rate")
            );

        TridentState streamRate = stream
            .aggregate(new Fields(), new StreamRateAggregator(2), new Fields("rate"))
            .partitionPersist(
                new AsyncHBaseStateFactory("hbase-cluster"),
                new Fields("rate"),
                new AsyncHBaseStateUpdater(updateMapper)
            ).parallelismHint(5);

        topology.newDRPCStream("stream rate drpc", drpc)
            .stateQuery(
                streamRate,
                new AsyncHBaseStateQuery(queryMapper)
                    .setValueDeserializer(new AsyncHBaseLongSerializer()),
                new Fields("rate"))
            .each(new Fields("rate"), new Debug());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(50);

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
                log.info("steam rate : " + drpc.execute("stream rate drpc", ""));
                Thread.sleep(1000);
            }
        }
    }
}
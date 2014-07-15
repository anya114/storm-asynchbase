/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.utils.AsyncHBaseClientFactory;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This is a TridentState implementation backed by HBase using AsyncHBase client.
 * If the cacheSize option is provided the State will include a LRU Cache.
 * </p>
 * <p>
 * Please look at storm.asynchbase.example.topology.AsyncHBaseTridentMapStateExampleTopology
 * for a concrete use case.
 * </p>
 *
 * @param <T> State type NON_TRANSACTIONAL / TRANSACTIONAL / OPAQUE.
 */
public class AsyncHBaseMapState<T> implements IBackingMap<T> {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseMapState.class);

    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    private final Options<T> options;
    private final Serializer<T> serializer;
    private final HBaseClient client;
    private final int partitionNum;

    public AsyncHBaseMapState(final Options<T> options, Map conf, int partitionNum) {
        this.options = options;
        this.serializer = options.serializer;
        this.partitionNum = partitionNum;
        this.client = AsyncHBaseClientFactory.getHBaseClient(conf, options.cluster);

        if (this.options.tableBytes == null) {
            this.options.tableBytes = this.options.table.getBytes();
        }
        if (this.options.columnFamilyBytes == null) {
            this.options.columnFamilyBytes = this.options.columnFamily.getBytes();
        }
        if (this.options.columnQualifierBytes == null) {
            this.options.columnQualifierBytes = this.options.columnQualifier.getBytes();
        }
    }

    /**
     * FACTORY METHODS
     */

    public static StateFactory opaque() {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(options);
    }

    public static StateFactory opaque(Options<OpaqueValue> opts) {

        return new Factory(StateType.OPAQUE, opts);
    }

    public static StateFactory transactional() {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(options);
    }

    public static StateFactory transactional(Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        Options<Object> options = new Options<Object>();
        return nonTransactional(options);
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, opts);
    }

    private byte[] toRowKey(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e) {
            throw new RuntimeException("IOException creating HBase row key.", e);
        }
        return bos.toByteArray();
    }

    /**
     * @param keys States to get.
     * @return States values.
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<Deferred<ArrayList<KeyValue>>> requests = new ArrayList<>(keys.size());
        final List<T> values = new ArrayList<T>();
        for (int i = 0; i < keys.size(); i++) {
            final Integer I = i; // map request result to the right index.
            values.add(null);    // placeholder for the request result.
            requests.add(client.get(new GetRequest(
                this.options.tableBytes,
                this.toRowKey(keys.get(i)),
                this.options.columnFamilyBytes,
                this.options.columnQualifierBytes
            )).addCallbacks(new Callback<ArrayList<KeyValue>, ArrayList<KeyValue>>() {
                @Override
                public ArrayList<KeyValue> call(ArrayList<KeyValue> keyValues) {
                    log.info(keyValues.toString());
                    synchronized (values) {
                        if (keyValues.size() > 0) {
                            values.set(I, serializer.deserialize(keyValues.get(0).value()));
                        } else {
                            values.set(I, null);
                        }
                    }
                    return null;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) {
                    synchronized (values) {
                        values.set(I, null);
                        handleFailure(ex);
                    }
                    return ex;
                }
            }));
        }

        for (Deferred<ArrayList<KeyValue>> request : requests) {
            try {
                request.joinUninterruptibly(this.options.timeout);
            } catch (Exception ex) {
                handleFailure(ex);
            }
        }

        return values;
    }

    /**
     * @param keys States to update.
     * @param values State values.
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        for (int i = 0; i < keys.size(); i++) {
            this.client.put(new PutRequest(
                this.options.tableBytes,
                this.toRowKey(keys.get(i)),
                this.options.columnFamilyBytes,
                this.options.columnQualifierBytes,
                this.serializer.serialize(values.get(i))
            )).addErrback(new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    handleFailure(ex);
                    return ex;
                }
            });
        }
    }

    private void handleFailure(Exception ex) {
        switch (this.options.failStrategy) {
            case LOG:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                break;
            case RETRY:
                throw new FailedException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
            case FAILFAST:
                throw new RuntimeException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
        }
    }

    /**
     * <ul>
     * <li>
     * cacheSize : size of the LRU CachedMap
     * </li>
     * <li>
     * timeout : timeout while doing multiget/multiput in milliseconds
     * </li>
     * <li>
     * cluster : AsyncHBase client to use.
     * </li>
     * <li>
     * table : table to use<br/>
     * columnFamily : columnFamily to use<br/>
     * columnQualifier : columnQualifier to use<br/>
     * </li>
     * <li>
     * FailStrategy : Modify the way strom will handle AsyncHBase failures.<br/>
     * LOG : Only log error and don't return any results.<br/>
     * RETRY : Ask the spout to replay the batch.<br/>
     * FAILFAST : Let the function crash.<br/>
     * null/NOOP : Do nothing.<br/>
     * http://svendvanderveken.wordpress.com/2014/02/05/error-handling-in-storm-trident-topologies/
     * </li>
     * </ul>
     */
    public static class Options<T> implements Serializable {
        public Serializer<T> serializer = null;
        public int cacheSize = 5000;
        public long timeout = 0;
        public String cluster;
        public String globalKey = "$HBASE_STATE_GLOBAL$";
        public String table;
        public String columnFamily;
        public String columnQualifier;
        public byte[] tableBytes;
        public byte[] columnFamilyBytes;
        public byte[] columnQualifierBytes;
        public FailStrategy failStrategy = FailStrategy.LOG;
        ;

        public enum FailStrategy {NOOP, LOG, FAILFAST, RETRY}
    }

    /**
     * <p>
     * This factory handle state creation.
     * </p>
     */
    protected static class Factory implements StateFactory {
        private StateType stateType;
        private Options options;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Factory(StateType stateType, Options options) {
            this.stateType = stateType;
            this.options = options;

            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (this.options.serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            log.info("Preparing HBase State for partition {} of {}.", partitionIndex + 1, numPartitions);
            IBackingMap state = new AsyncHBaseMapState(options, conf, partitionIndex);

            if (options.cacheSize > 0) {
                state = new CachedMap(state, options.cacheSize);
            }

            MapState mapState;
            switch (stateType) {
                case NON_TRANSACTIONAL:
                    mapState = NonTransactionalMap.build(state);
                    break;
                case OPAQUE:
                    mapState = OpaqueMap.build(state);
                    break;
                case TRANSACTIONAL:
                    mapState = TransactionalMap.build(state);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown state type: " + stateType);
            }
            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }

    }
}

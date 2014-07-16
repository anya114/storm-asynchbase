/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.utils.serializer.AsyncHBaseDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This bolt extract fields from List<KeyValue> returned by AsyncHBase GetRequests.<br/>
 * Output tuple fields order is RowKey,ColumnFamily,ColumnQualifier,Value,Timestamp.<br/>
 * You can select which fields you want to be returned in the constructor.
 * You may provide deserializers to map byte[] to the type you want. By default it tries to
 * map result to String. So you need to provide a serializer if you want to keep byte[],
 * shame but string is still the most used type and it's not hard to override this behaviour.
 * </p>
 */
public class ExtractKeyValuesBolt extends BaseRichBolt {
        public static final Logger log = LoggerFactory.getLogger(ExtractKeyValuesBolt.class);

        private final boolean rowKey;
        private final boolean family;
        private final boolean qualifier;
        private final boolean value;
        private final boolean timestamp;

        private AsyncHBaseDeserializer rowKeyDeserializer;
        private AsyncHBaseDeserializer columnFamilyDeserializer;
        private AsyncHBaseDeserializer columnQualifierDeserializer;
        private AsyncHBaseDeserializer valueDeserializer;

        private OutputCollector collector;

        /**
         * @param rowKey    return rowKey.
         * @param family    return columnFamily.
         * @param qualifier return qualifier.
         * @param value     return value.
         * @param timestamp return timestamp.
         */
        public ExtractKeyValuesBolt(boolean rowKey, boolean family, boolean qualifier,
                                boolean value, boolean timestamp) {
            this.rowKey = rowKey;
            this.family = family;
            this.qualifier = qualifier;
            this.value = value;
            this.timestamp = timestamp;
        }

        /**
         * <p>
         * Default constructor, return ColumnQualifier and Value.
         * </p>
         */
        public ExtractKeyValuesBolt() {
            this(false, false, true, true, false);
        }

        /**
         * @param rowKeyDeserializer Deserializer to use to map rowKey to a specific type.
         * @return This so you can do method chaining.
         */
        public ExtractKeyValuesBolt setRowKeyDeserializer(AsyncHBaseDeserializer rowKeyDeserializer) {
            this.rowKeyDeserializer = rowKeyDeserializer;
            return this;
        }

        /**
         * @param columnFamilyDeserializer Deserializer to use to map columnFamily to a specific type.
         * @return This so you can do method chaining.
         */
        public ExtractKeyValuesBolt setColumnFamilyDeserializer(AsyncHBaseDeserializer columnFamilyDeserializer) {
            this.columnFamilyDeserializer = columnFamilyDeserializer;
            return this;
        }

        /**
         * @param columnQualifierDeserializer Deserializer to use to map columnQualifier to a specific type.
         * @return This so you can do method chaining.
         */
        public ExtractKeyValuesBolt setColumnQualifierDeserializer(AsyncHBaseDeserializer columnQualifierDeserializer) {
            this.columnQualifierDeserializer = columnQualifierDeserializer;
            return this;
        }

        /**
         * @param valueDeserializer Deserializer to use to map cell value to a specific type.
         * @return This so you can do method chaining.
         */
        public ExtractKeyValuesBolt setValueDeserializer(AsyncHBaseDeserializer valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public void execute(Tuple tuple) {
            List<KeyValue> keyValueList = (List<KeyValue>) tuple.getValue(0);
            for (KeyValue keyValue : keyValueList) {
                Values values = new Values();
                if (this.rowKey) {
                    if (this.rowKeyDeserializer != null) {
                        values.add(this.rowKeyDeserializer.deserialize(keyValue.key()));
                    } else {
                        values.add(new String(keyValue.key()));
                    }
                }
                if (this.family) {
                    if (this.columnFamilyDeserializer != null) {
                        values.add(this.columnFamilyDeserializer.deserialize(keyValue.family()));
                    } else {
                        values.add(new String(keyValue.family()));
                    }
                }
                if (this.qualifier) {
                    if (this.columnQualifierDeserializer != null) {
                        values.add(this.columnFamilyDeserializer.deserialize(keyValue.qualifier()));
                    } else {
                        values.add(new String(keyValue.qualifier()));
                    }
                }
                if (this.value) {
                    if (this.valueDeserializer != null) {
                        values.add(this.valueDeserializer.deserialize(keyValue.value()));
                    } else {
                        values.add(new String(keyValue.value()));
                    }
                }
                if (this.timestamp) {
                    values.add(keyValue.timestamp());
                }
                collector.emit(values);
            }
        }

        /**
         * <p>
         * Initialize serializers
         * </p>
         * @param conf Topology configuration
         * @param context Operation context
         */

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            if (this.rowKeyDeserializer != null) {
                this.rowKeyDeserializer.prepare(conf);
            }
            if (this.columnFamilyDeserializer != null) {
                this.columnFamilyDeserializer.prepare(conf);
            }
            if (this.columnQualifierDeserializer != null) {
                this.columnQualifierDeserializer.prepare(conf);
            }
            if (this.valueDeserializer != null) {
                this.valueDeserializer.prepare(conf);
            }
        }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ArrayList<String> fields = new ArrayList<>();
        if (this.rowKey) {
            fields.add("rowkey");
        }
        if (this.family) {
            fields.add("family");
        }
        if (this.qualifier) {
            fields.add("qualifier");
        }
        if (this.value) {
            fields.add("value");
        }
        if (this.timestamp) {
            fields.add("timestamp");
        }
        declarer.declare(new Fields(fields));
    }
}

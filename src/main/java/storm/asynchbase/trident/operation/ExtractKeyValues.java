/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.operation;

import backtype.storm.tuple.Values;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.utils.serializer.AsyncHBaseDeserializer;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This function extract fields from List<KeyValue> returned by AsyncHBase GetRequests.<br/>
 * Output tuple fields order is RowKey,ColumnFamily,ColumnQualifier,Value,Timestamp.<br/>
 * You can select which fields you want to be returned in the constructor.
 * You may provide deserializers to map byte[] to the type you want. By default it tries to
 * map result to String. So you need to provide a serializer if you want to keep byte[],
 * shame but string is still the most used type and it's not hard to override this behaviour.
 * </p>
 */
public class ExtractKeyValues extends BaseFunction {
    public static final Logger log = LoggerFactory.getLogger(ExtractKeyValues.class);

    private final boolean rowKey;
    private final boolean family;
    private final boolean qualifier;
    private final boolean value;
    private final boolean timestamp;

    private AsyncHBaseDeserializer rowKeyDeserializer;
    private AsyncHBaseDeserializer columnFamilyDeserializer;
    private AsyncHBaseDeserializer columnQualifierDeserializer;
    private AsyncHBaseDeserializer valueDeserializer;

    /**
     * @param rowKey    return rowKey.
     * @param family    return columnFamily.
     * @param qualifier return qualifier.
     * @param value     return value.
     * @param timestamp return timestamp.
     */
    public ExtractKeyValues(boolean rowKey, boolean family, boolean qualifier,
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
    public ExtractKeyValues() {
        this(false, false, true, true, false);
    }

    /**
     * @param rowKeyDeserializer Deserializer to use to map rowKey to a specific type.
     * @return This so you can do method chaining.
     */
    public ExtractKeyValues setRowKeyDeserializer(AsyncHBaseDeserializer rowKeyDeserializer) {
        this.rowKeyDeserializer = rowKeyDeserializer;
        return this;
    }

    /**
     * @param columnFamilyDeserializer Deserializer to use to map columnFamily to a specific type.
     * @return This so you can do method chaining.
     */
    public ExtractKeyValues setColumnFamilyDeserializer(AsyncHBaseDeserializer columnFamilyDeserializer) {
        this.columnFamilyDeserializer = columnFamilyDeserializer;
        return this;
    }

    /**
     * @param columnQualifierDeserializer Deserializer to use to map columnQualifier to a specific type.
     * @return This so you can do method chaining.
     */
    public ExtractKeyValues setColumnQualifierDeserializer(AsyncHBaseDeserializer columnQualifierDeserializer) {
        this.columnQualifierDeserializer = columnQualifierDeserializer;
        return this;
    }

    /**
     * @param valueDeserializer Deserializer to use to map cell value to a specific type.
     * @return This so you can do method chaining.
     */
    public ExtractKeyValues setValueDeserializer(AsyncHBaseDeserializer valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void execute(TridentTuple tuple, TridentCollector collector) {
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
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
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
}

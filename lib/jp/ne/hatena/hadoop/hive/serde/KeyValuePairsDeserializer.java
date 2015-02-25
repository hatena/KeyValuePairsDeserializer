package jp.ne.hatena.hadoop.hive.serde;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * KeyValueDeserializer
 *
 * データの各行がキーとバリューの組でできている場合に便利なDeserializer.
 *
 * キーとバリューは kvSeparator で分割され、各組は fieldSeparator で分割される.
 * テーブル定義からキーに該当するカラムがあった場合、自動でバリューをそのカラムに割当て、
 * 該当するカラムが無い場合は、そのバリューは無視される.
 *
 * 例えば、次のようなテーブルがあるとする:
 *
 * CREATE TABLE access_log (
 *     time STRING,
 *     host STRING
 * ) ROW FORMAT SERDE 'jp.ne.hatena.hadoop.hive.serde.KeyValuePairsDeserializer';
 *
 * これに対し、次のようなデータをロードする:
 *
 * time:[28/Apr/2011:12:00:00 +900]	host:66.249.69.181	status:302
 * time:[28/Apr/2011:12:00:00 +900]	status:200
 *
 * この場合、access_logテーブルは次のようになる:
 *
 * +---------------------------+-------------+
 * |           time            |    host     |
 * +===========================+=============+
 * |[28/Apr/2011:12:00:00 +900]|66.249.69.181|
 * |[28/Apr/2011:12:00:00 +900]|    null     |
 * +---------------------------+-------------+
 *
 * @author yuku_t
 * @version 0.1.0
 *
 */
public class KeyValuePairsDeserializer implements Deserializer {
    /** Apache commons logger */
    private static final Log LOG = LogFactory.getLog(KeyValuePairsDeserializer.class.getName());

    /** Default field separator */
    private static String fieldSeparator = "\t";

    /** Default Key Value separator */
    private static String kvSeparator = ":";

    /** The number of columns in the table this SerDe is being used with */
    private int numColumns;

    /** Hash from column name to it's index */
    private Map<String, Integer> columnIndexMap;

    /** An ObjectInspector to be used as meta-data about a deserialized row */
    private StructObjectInspector rowOI;

    /** A row object */
    private ArrayList<Object> row;

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        LOG.debug("Deserialize row: " + rowText.toString());

        // Reset row
        for (int i = 0; i < numColumns; i++) {
            row.set(i, null);
        }

        // Split a line into key value pair string with field separator
        List<String> keyValueList = Arrays.asList(rowText.toString().split(fieldSeparator));
        List<? extends StructField> rowFieldRefs = rowOI.getAllStructFieldRefs();

        // Parse each key value pair string
        for (String keyValueString : keyValueList) {
            if (!keyValueString.contains(kvSeparator)) continue;

            String[] keyValue = keyValueString.split(kvSeparator, 2);
            Integer colIndex = columnIndexMap.get(keyValue[0].toLowerCase());
            if (colIndex != null) {
                ObjectInspector fieldOI = rowFieldRefs.get(colIndex).getFieldObjectInspector();
                // FIXME: もっとスマートにやりたい
                Object val = null;
                try {
                    if (fieldOI.getTypeName().equals("string")) {
                        val = keyValue[1];
                    } else if (fieldOI.getTypeName().equals("int")) {
                        val = Integer.parseInt(keyValue[1]);
                    } else if (fieldOI.getTypeName().equals("bigint")) {
                        val = new BigInteger(keyValue[1]);
                    } else if (fieldOI.getTypeName().equals("float")) {
                        val = Float.parseFloat(keyValue[1]);
                    } else if (fieldOI.getTypeName().equals("double")) {
                        val = Double.parseDouble(keyValue[1]);
                    } else if (fieldOI.getTypeName().equals("boolean")) {
                        val = Boolean.parseBoolean(keyValue[1]);
                    }
                } catch (RuntimeException e) {
                    val = null;
                }
                row.set(colIndex, val);
            }
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // no support for statistics yet
        return null;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        LOG.debug("Initializing KeyValuePairsDeserializer");

        // Read the configuration parameters
        String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        // Set custom separator
        String inputFieldSeparator = tbl.getProperty("input.fieldSeparator");
        if (inputFieldSeparator != null) {
            fieldSeparator = inputFieldSeparator;
        }
        String inputKVSeparator = tbl.getProperty("input.kvSeparator");
        if (inputKVSeparator != null) {
            kvSeparator = inputKVSeparator;
        }

        // Create HashMap from column name to it's index in the table definition.
        columnIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < numColumns; i++) {
            String colName = columnNames.get(i);
            columnIndexMap.put(colName, i);
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numColumns);
        ObjectInspector oi;
        for (TypeInfo ti : columnTypes) {
            oi = TypeInfoUtils
                    .getStandardJavaObjectInspectorFromTypeInfo(ti);
            columnOIs.add(oi);
        }
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            row.add(null);
        }

        LOG.debug("KeyValuePairsDeserializer initialization complete");
    }
}

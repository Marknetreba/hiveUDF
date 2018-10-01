package udf;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Arrays;

public class JavaUDF extends UDF {

    public ArrayList checkIP(String network) {
        SubnetUtils utils = new SubnetUtils(network);
        return new ArrayList(Arrays.asList(utils.getInfo().getAllAddresses()));
    }
}

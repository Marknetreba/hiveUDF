package udf;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class JavaUDF extends UDF {

    public String[] checkIP(String network) {
        SubnetUtils utils = new SubnetUtils(network);
        return utils.getInfo().getAllAddresses();
    }
}

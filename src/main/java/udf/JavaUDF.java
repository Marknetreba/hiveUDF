package udf;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class JavaUDF extends UDF {

    static File in;
    static DatabaseReader reader;

    static {
        in = new File("/Users/mnetreba/Downloads/mmdb/countries.mmdb");
        try {
            reader = new DatabaseReader.Builder(in).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String evaluate(String ip) throws GeoIp2Exception, IOException {

        InetAddress ipAddress = InetAddress.getByName(ip);
        try {
            CountryResponse response = reader.country(ipAddress);
            return response.getCountry().getNames().get("en");
        } catch (AddressNotFoundException ex) {
            return "";
        }
    }
}

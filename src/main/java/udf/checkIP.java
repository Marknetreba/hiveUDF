package udf;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.net.InetAddress;

public class checkIP extends UDF {

    static FileSystem fs;
    static FSDataInputStream in;
    static DatabaseReader reader;

    static {
        try {
            fs = FileSystem.get(new Configuration());
            in = fs.open(new Path("/tmp/countries.mmdb"));
            reader = new DatabaseReader.Builder(in).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String evaluate(String ip) throws IOException, GeoIp2Exception {
        InetAddress ipAddress = InetAddress.getByName(ip);
        try {
            CountryResponse response = reader.country(ipAddress);
            return response.getCountry().getNames().get("en");
        }
        catch (AddressNotFoundException ex) {
            return "";
        }
    }
}

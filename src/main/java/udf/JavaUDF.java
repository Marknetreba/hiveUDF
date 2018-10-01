package udf;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class JavaUDF {

    public String checkIP(String ip) throws IOException, GeoIp2Exception {

        File database = new File("/Users/mnetreba/Downloads/mmdb/countries.mmdb");
        DatabaseReader reader = new DatabaseReader.Builder(database).build();
        InetAddress ipAddress = InetAddress.getByName(ip);
        CountryResponse response = reader.country(ipAddress);
        return response.getCountry().getNames().get("en");
    }
}

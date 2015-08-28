package com.pinterest.terrapin.client;

import com.google.common.collect.Lists;
import com.pinterest.terrapin.thrift.generated.TerrapinGetRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinService;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.ByteBuffer;

/**
 * Simple tool for getting the value for a key which has been loaded to terrapin.
 *
 * Usage (Run from repository root):
 *
 * mvn clean package -pl client -am
 * java -cp client/target/*:client/target/lib/* \
 *     -Dconfig={properties_file}               \
 *     com.pinterest.terrapin.client.ClientTool {fileset} {key}
 *
 * The zookeeper quorum and the cluster name are picked from the properties file.
 */
public class ClientTool {
  public static void main(String[] args) throws Exception {
    PropertiesConfiguration config = new PropertiesConfiguration(System.getProperty("config"));
    TerrapinClient client = new TerrapinClient(config, 9090, 1000, 5000);
    String key = args[1];
    TerrapinSingleResponse response = client.getOne(args[0],  // fileset
        ByteBuffer.wrap(key.getBytes())).get();
    if (response.isSetErrorCode()) {
      System.out.println("Got error " + response.getErrorCode().toString());
    } else if (response.isSetValue()) {
      System.out.println("Got value.");
      System.out.println(new String(response.getValue()));
    } else {
      System.out.println("Key " + key + " not found.");
    }
    System.exit(0);
  }
}

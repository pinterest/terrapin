/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.pinterest.terrapin.tools;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.PartitionerFactory;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class HFileGenerator {
  private static final String PARTITIONER_TYPE_OPTION = "t";
  private static final String NUM_OF_PARTITIONS_OPTION = "p";
  private static final String NUM_OF_KEYS_OPTION = "k";
  private static final String HELP_OPTION = "h";
  private static final PartitionerType DEFAULT_PARTITIONER_TYPE = PartitionerType.MODULUS;
  private static final int DEFAULT_NUM_OF_PARTITIONS = 10;
  private static final int DEFAULT_NUM_OF_KEYS = 1000;

  private static Options getCommandLineOptions() {
    Options options = new Options();
    options.addOption(PARTITIONER_TYPE_OPTION, "type", true,
        String.format("type of partitioner [MODULUS,CASCADING], default %s",
            DEFAULT_PARTITIONER_TYPE.name()));
    options.addOption(NUM_OF_PARTITIONS_OPTION, "partitions", true,
        String.format("number of partitions, default %d", DEFAULT_NUM_OF_PARTITIONS));
    options.addOption(NUM_OF_KEYS_OPTION, "keys", true,
        String.format("number of keys per partition, default %d", DEFAULT_NUM_OF_KEYS));
    options.addOption(HELP_OPTION, "help", false, "print usage message");
    return options;
  }

  private static void printHelpMessage(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("HFileGenerator [options] OUTPUT_DIR", options);
  }

  private static PartitionerType getPartitionerType(CommandLine cmd) {
    PartitionerType partitionerType = DEFAULT_PARTITIONER_TYPE;
    if (cmd.hasOption(PARTITIONER_TYPE_OPTION)) {
      String partitionerTypeStr = cmd.getOptionValue(PARTITIONER_TYPE_OPTION);
      if (partitionerTypeStr.equalsIgnoreCase(PartitionerType.MODULUS.name())) {
        partitionerType = PartitionerType.MODULUS;
      } else if (partitionerTypeStr.equalsIgnoreCase(PartitionerType.CASCADING.name())) {
        partitionerType = PartitionerType.CASCADING;
      } else {
        throw new IllegalArgumentException("wrong partitioner type");
      }
    }
    return partitionerType;
  }

  private static int getNumOfPartitions(CommandLine cmd) {
    int numOfPartitions = DEFAULT_NUM_OF_PARTITIONS;
    if (cmd.hasOption(NUM_OF_PARTITIONS_OPTION)) {
      try {
        numOfPartitions = Integer.valueOf(cmd.getOptionValue(NUM_OF_PARTITIONS_OPTION));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("number of partition should be a valid number");
      }
      if (numOfPartitions <= 0) {
        throw new IllegalArgumentException("number of partition should greater than 0");
      }
    }
    return numOfPartitions;
  }

  private static int getNumOfKeys(CommandLine cmd) {
    int numOfKeys = DEFAULT_NUM_OF_KEYS;
    if (cmd.hasOption(NUM_OF_KEYS_OPTION)) {
      try {
        numOfKeys = Integer.valueOf(cmd.getOptionValue(NUM_OF_KEYS_OPTION));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("number of keys should be a valid number");
      }
      if (numOfKeys <= 0) {
        throw new IllegalArgumentException("number of keys should greater than 0");
      }
    }
    return numOfKeys;
  }

  private static boolean hasHelp(CommandLine cmd) {
    return cmd.hasOption(HELP_OPTION);
  }

  private static void checkOutputFolder(File outputFolder) {
    if(!outputFolder.exists() && !outputFolder.mkdirs()) {
      throw new IllegalArgumentException(String.format("can not create dir %s",
          outputFolder.getAbsolutePath()));
    }
  }

  /**
   * Generate hfiles for testing purpose
   *
   * @param sourceFileSystem source file system
   * @param conf configuration for hfile
   * @param outputFolder output folder for generated hfiles
   * @param partitionerType partitioner type
   * @param numOfPartitions number of partitions
   * @param numOfKeys number of keys
   * @return list of generated hfiles
   * @throws IOException if hfile creation goes wrong
   */
  public static List<Path> generateHFiles(FileSystem sourceFileSystem, Configuration conf,
                                          File outputFolder, PartitionerType partitionerType,
                                          int numOfPartitions, int numOfKeys)
      throws IOException {
    StoreFile.Writer[] writers = new StoreFile.Writer[numOfPartitions];
    for (int i = 0; i < numOfPartitions; i++) {
      writers[i] = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), sourceFileSystem, 4096)
          .withFilePath(new Path(String.format("%s/%s", outputFolder.getAbsoluteFile(),
              TerrapinUtil.formatPartitionName(i))))
          .withCompression(Compression.Algorithm.NONE)
          .build();
    }
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerType);
    for (int i = 0; i < numOfKeys; i++) {
      byte[] key = String.format("%06d", i).getBytes();
      byte[] value;
      if (i <= 1) {
        value = "".getBytes();
      } else {
        value = ("v" + (i + 1)).getBytes();
      }
      KeyValue kv = new KeyValue(key, Bytes.toBytes("cf"), Bytes.toBytes(""), value);
      int partition = partitioner.getPartition(new BytesWritable(key), new BytesWritable(value),
          numOfPartitions);
      writers[partition].append(kv);
    }
    for (int i = 0; i < numOfPartitions; i++) {
      writers[i].close();
    }
    return Lists.transform(Lists.newArrayList(writers), new Function<StoreFile.Writer, Path>() {
      @Override
      public Path apply(StoreFile.Writer writer) {
        return writer.getPath();
      }
    });
  }

  public static void main(String[] args) {
    Options options = getCommandLineOptions();
    CommandLineParser cmdParser = new PosixParser();
    if (args.length < 1) {
      printHelpMessage(options);
    } else {
      File outputFolder = new File(args[args.length - 1]);
      checkOutputFolder(outputFolder);
      String[] optionArgs = new String[args.length - 1];
      System.arraycopy(args, 0, optionArgs, 0, args.length - 1);
      try {
        CommandLine cmd = cmdParser.parse(options, optionArgs);
        if (hasHelp(cmd)) {
          printHelpMessage(options);
        } else {
          Configuration conf = new Configuration();
          FileSystem fs = FileSystem.get(conf);
          generateHFiles(fs, conf, outputFolder,
              getPartitionerType(cmd), getNumOfPartitions(cmd), getNumOfKeys(cmd));
        }
      } catch (ParseException e) {
        printHelpMessage(options);
      } catch (IllegalArgumentException e) {
        System.out.println(e.getMessage());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

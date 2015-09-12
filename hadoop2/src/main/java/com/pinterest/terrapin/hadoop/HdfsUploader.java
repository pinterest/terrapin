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
package com.pinterest.terrapin.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.TerrapinUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Class for uploading data existing on HDFS.
 */
public class HdfsUploader extends BaseUploader {
  private static final Logger LOG = LoggerFactory.getLogger(S3Uploader.class);

  private final DFSClient dfsClient;
  private final Path hdfsDir;

  public HdfsUploader(TerrapinUploaderOptions uploaderOptions,
                      String absoluteHdfsDir) throws IOException, URISyntaxException {
    super(uploaderOptions);
    Path hdfsPathTmp = new Path(absoluteHdfsDir);
    URI namenodeUri = new URI(
        hdfsPathTmp.toUri().getScheme(), hdfsPathTmp.toUri().getAuthority(), null, null);
    this.dfsClient = new DFSClient(namenodeUri, new Configuration());
    this.hdfsDir = new Path(hdfsPathTmp.toUri().getPath());
  }

  @Override
  List<Pair<Path, Long>> getFileList() {
    List<Pair<Path, Long>> fileSizePairList = Lists.newArrayList();
    try {
      List<HdfsFileStatus> fileStatusList = TerrapinUtil.getHdfsFileList(dfsClient, hdfsDir.toString());
      for (HdfsFileStatus fileStatus : fileStatusList) {
        fileSizePairList.add(new ImmutablePair(fileStatus.getFullPath(hdfsDir), fileStatus.getLen()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return fileSizePairList;
  }

  public static void main(String[] args) {
    TerrapinUploaderOptions uploaderOptions = TerrapinUploaderOptions.initFromSystemProperties();
    uploaderOptions.validate();

    String hdfsDir = System.getProperties().getProperty("terrapin.hdfs_dir");
    Preconditions.checkNotNull(hdfsDir);

    try {
      new HdfsUploader(uploaderOptions, hdfsDir).upload(uploaderOptions.terrapinCluster,
          uploaderOptions.terrapinFileSet, uploaderOptions.loadOptions);
    } catch (Exception e) {
      LOG.error("Upload FAILED.", e);
      System.exit(1);
    }
    // We need to force an exit since some of the netty threads instantiated as part
    // of the process are not daemon threads.
    System.exit(0);
  }
}

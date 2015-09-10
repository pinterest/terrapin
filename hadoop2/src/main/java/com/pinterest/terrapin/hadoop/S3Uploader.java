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

import com.amazonaws.auth.BasicAWSCredentials;
import com.google.common.base.Preconditions;
import com.pinterest.terrapin.TerrapinUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class for uploading data existing on S3.
 */
public class S3Uploader extends BaseUploader {
  private static final Logger LOG = LoggerFactory.getLogger(S3Uploader.class);

  private final String s3Bucket;
  private final String s3KeyPrefix;

  public S3Uploader(TerrapinUploaderOptions uploaderOptions,
                    String s3Bucket,
                    String s3KeyPrefix) {
    super(uploaderOptions);
    this.s3Bucket = s3Bucket;
    this.s3KeyPrefix = s3KeyPrefix;
  }

  @Override
  List<Pair<Path, Long>> getFileList() {
    return TerrapinUtil.getS3FileList(new BasicAWSCredentials(
        conf.get("fs.s3n.awsAccessKeyId"),
        conf.get("fs.s3n.awsSecretAccessKey")), s3Bucket, s3KeyPrefix);
  }

  public static void main(String[] args) {
    TerrapinUploaderOptions uploaderOptions = TerrapinUploaderOptions.initFromSystemProperties();
    uploaderOptions.validate();

    String s3Bucket = System.getProperties().getProperty("terrapin.s3bucket");
    String s3Prefix = System.getProperties().getProperty("terrapin.s3key_prefix");
    Preconditions.checkNotNull(s3Bucket);
    Preconditions.checkNotNull(s3Prefix);

    try {
      new S3Uploader(uploaderOptions, s3Bucket, s3Prefix).upload(uploaderOptions.terrapinCluster,
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

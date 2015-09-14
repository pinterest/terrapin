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
package com.pinterest.terrapin.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for FileSetViewManager.
 */
public class FileSetViewManagerTest {
  private static final String FILE_SET = "file_set";

  private FileSetViewManager manager;

  @Before
  public void setUp() {
    manager = new FileSetViewManager("test");
  }

  private void triggerFileSetInfoWatch(String fileSet, FileSetInfo fsInfo) throws Exception {
    manager.zkBackedFileSetInfoMap.put(fileSet, manager.fileSetInfoFunction.apply(fsInfo.toJson()));
  }

  private void triggerViewInfoWatch(ViewInfo viewInfo) throws Exception {
    manager.zkBackedViewInfoMap.put(viewInfo.getResource(),
        manager.viewInfoFunction.apply(viewInfo.toCompressedJson()));
  }

  @Test
  public void testWithBackup() throws Exception {
    boolean exception = false;
    try {
      manager.getFileSetViewInfo(FILE_SET);
    } catch (TerrapinGetException e) {
      assertEquals(TerrapinGetErrorCode.FILE_SET_NOT_FOUND, e.getErrorCode());
      exception = true;
    }
    assertTrue(exception);
    // Now, a fileset watch is triggered but the external view is not triggered.
    FileSetInfo fsInfo1 = new FileSetInfo(
        "file_set", "/terrapin/data/file_set/1", 1, (List)Lists.newArrayList(), new Options());
    triggerFileSetInfoWatch(FILE_SET, fsInfo1);
    exception = false;
    try {
      manager.getFileSetViewInfo(FILE_SET);
    } catch (TerrapinGetException e) {
      assertEquals(TerrapinGetErrorCode.INVALID_FILE_SET_VIEW, e.getErrorCode());
      exception = true;
    }
    assertTrue(exception);

    // Now the watch for the first view is triggered.
    ExternalView externalView1 = new ExternalView("$terrapin$data$file_set$1");
    externalView1.setStateMap("0", ImmutableMap.of("host1", "ONLINE"));
    ViewInfo viewInfo1 = new ViewInfo(externalView1);
    triggerViewInfoWatch(viewInfo1);

    Pair<FileSetInfo, ViewInfo> pair = manager.getFileSetViewInfo(FILE_SET);
    assertEquals(fsInfo1, pair.getLeft());
    assertEquals(viewInfo1, pair.getRight());

    // The fileset info alone gets updated but the other watch is not yet triggered.
    FileSetInfo fsInfo2 = new FileSetInfo(
        FILE_SET, "/terrapin/data/file_set/2", 1, (List)Lists.newArrayList(), new Options());
    triggerFileSetInfoWatch(FILE_SET, fsInfo2);
    pair = manager.getFileSetViewInfo(FILE_SET);
    // We should see the view from the backup only.
    assertEquals(fsInfo1, pair.getLeft());
    assertEquals(viewInfo1, pair.getRight());

    // Finally, a watch is triggered for the resource corresponding to the second fileset.
    ExternalView externalView2 = new ExternalView("$terrapin$data$file_set$2");
    externalView2.setStateMap("0", ImmutableMap.of("host2", "ONLINE"));
    ViewInfo viewInfo2 = new ViewInfo(externalView2);
    triggerViewInfoWatch(viewInfo2);

    pair = manager.getFileSetViewInfo(FILE_SET);
    assertEquals(fsInfo2, pair.getLeft());
    assertEquals(viewInfo2, pair.getRight());

    // Watches execute in the correct order. Firs the view is propagated.
    ExternalView externalView3 = new ExternalView("$terrapin$data$file_set$3");
    externalView3.setStateMap("0", ImmutableMap.of("host3", "ONLINE"));
    ViewInfo viewInfo3 = new ViewInfo(externalView3);
    triggerViewInfoWatch(viewInfo3);

    // We should still continue to serve previous view.
    pair = manager.getFileSetViewInfo(FILE_SET);
    assertEquals(fsInfo2, pair.getLeft());
    assertEquals(viewInfo2, pair.getRight());

    // Finally, the fileset version update watch is propagated.
    FileSetInfo fsInfo3 = new FileSetInfo(
        FILE_SET, "/terrapin/data/file_set/3", 1, (List)Lists.newArrayList(), new Options());
    triggerFileSetInfoWatch(FILE_SET, fsInfo3);

    pair = manager.getFileSetViewInfo(FILE_SET);
    assertEquals(fsInfo3, pair.getLeft());
    assertEquals(viewInfo3, pair.getRight());
  }
}

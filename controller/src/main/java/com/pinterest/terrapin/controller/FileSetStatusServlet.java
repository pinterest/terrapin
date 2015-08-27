package com.pinterest.terrapin.controller;

import com.google.common.collect.Lists;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a servlet to check status for a specific file set
 */
public class FileSetStatusServlet extends HttpServlet {

  public static final String BASE_URI = "/status/file_sets";
  private static final Pattern uriPattern = Pattern.compile(BASE_URI + "/(.*)");
  private static final Logger LOG = LoggerFactory.getLogger(FileSetStatusServlet.class);

  /**
   * Get file set name from a giving uri.
   * E.g. Giving '/status/file_sets/image_scores', it returns 'image_scores'
   * @param uri the uri to be parsed
   * @return file set name
   */
  public static String parseFileSetFromURI(String uri){
    Matcher matcher = uriPattern.matcher(uri);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }

  /**
   * Get partition map for a specific file set as pretty printing JSON string
   * @param zkManager ZookeeperManager instance
   * @param fileSetInfo file set information
   * @return JSON string
   * @throws IOException if dumping process raises any exceptions.
   */
  public static String getPartitionMapJsonString(ZooKeeperManager zkManager,
                                                 FileSetInfo  fileSetInfo)
      throws IOException {
    if (fileSetInfo.servingInfo == null) {
      return "{}";
    }
    ViewInfo viewInfo = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
    if (viewInfo == null) {
      return "{}";
    }
    return viewInfo.toPrettyPrintingJson();
  }

  /**
   * Get all missing partitions
   * @param zkManager ZookeeperManager instance
   * @param fileSetInfo file set information
   * @return list of missing partitions
   */
  public static List<Integer> getMissingPartitions(ZooKeeperManager zkManager,
                                                  FileSetInfo fileSetInfo) {
    if (fileSetInfo.servingInfo == null) {
      return Lists.newArrayListWithCapacity(0);
    }
    ViewInfo viewInfo = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
    if (viewInfo == null) {
      return Lists.newArrayListWithCapacity(0);
    }
    List<Integer> missingPartitions = new ArrayList<Integer>();
    for (int i = 0; i < fileSetInfo.servingInfo.numPartitions; i++) {
      String partitionName = TerrapinUtil.getViewPartitionName(
          fileSetInfo.servingInfo.helixResource, i);
      if (!viewInfo.isOnlinePartition(partitionName)) {
        missingPartitions.add(i);
      }
    }
    return missingPartitions;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    response.setContentType("text/html");
    ServletContext context = getServletContext();

    ZooKeeperManager zkManager = (ZooKeeperManager) context.getAttribute("zookeeper-manager");
    String clusterName = (String) context.getAttribute("cluster_name");
    String fileSet = parseFileSetFromURI(request.getRequestURI());
    FileSetInfo fileSetInfo = zkManager.getFileSetInfo(fileSet);
    String rawJson = fileSetInfo.toPrettyPrintingJson();
    String partitionMapJson = getPartitionMapJsonString(zkManager, fileSetInfo);

    FileSetStatusTmpl fileSetStatusTmpl = new FileSetStatusTmpl();
    fileSetStatusTmpl.render(
        response.getWriter(),
        clusterName,
        fileSet,
        rawJson,
        partitionMapJson,
        getMissingPartitions(zkManager, fileSetInfo));
  }
}

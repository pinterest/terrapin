package com.pinterest.terrapin.controller;

import com.pinterest.terrapin.base.BytesUtil;
import com.pinterest.terrapin.client.TerrapinClient;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LookupKeyServlet extends HttpServlet {

  public static final String BASE_URI = "/status/lookup";
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  public enum LookupKeyStatus {
    OK, MISSING, ERROR
  }

  public static class LookupKeyResponse {
    public LookupKeyStatus status;
    public String value;
    public LookupKeyResponse(LookupKeyStatus status, String value) {
      this.status = status;
      this.value = value;
    }
    public LookupKeyResponse(LookupKeyStatus status) {
      this.status = status;
    }
  }

  private Map<String, LookupKeyResponse> lookupKeys(String fileSet, String keys) {
    if (fileSet == null || keys == null || fileSet.length() == 0 || keys.length() == 0) {
      return ImmutableMap.of();
    }

    Map<String, LookupKeyResponse> results = Maps.newHashMap();
    Set<ByteBuffer> keySet = Sets.newHashSet();

    for (String key : keys.split("[\n]+")) {
      if (key.length() > 0) {
        results.put(key, new LookupKeyResponse(LookupKeyStatus.MISSING));
        keySet.add(ByteBuffer.wrap(key.getBytes()));
      }
    }

    ServletContext context = getServletContext();
    TerrapinClient client = (TerrapinClient) context.getAttribute("sample-client");
    TerrapinResponse response = client.getMany(fileSet, keySet).apply();

    for (Map.Entry<ByteBuffer, TerrapinSingleResponse> entry :
        response.getResponseMap().entrySet()) {
      String key = new String(BytesUtil.readBytesFromByteBuffer(entry.getKey()));
      TerrapinSingleResponse value = entry.getValue();
      if (value.isSetErrorCode()) {
        results.put(key, new LookupKeyResponse(LookupKeyStatus.ERROR,
            value.getErrorCode().name()));
      } else {
        results.put(key, new LookupKeyResponse(LookupKeyStatus.OK,
            new String(value.getValue())));
      }
    }
    return results;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String fileSet = req.getParameter("fileset");
    String keys = req.getParameter("keys");
    resp.setContentType("application/json");
    resp.getWriter().append(
        JSON_MAPPER.writeValueAsString(lookupKeys(fileSet, keys))
    );
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used by the lagging Journal service to retrieve edit file from
 * another Journal service for sync up.
 * TODO update this
 */
@InterfaceAudience.Private
public class GetJournalEditServlet extends HttpServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Log LOG = LogFactory.getLog(GetJournalEditServlet.class);

  private static final String STORAGEINFO_PARAM = "storageInfo";
  private static final String JOURNAL_ID_PARAM = "jid";
  private static final String SEGMENT_TXID_PARAM = "segmentTxId";

  // TODO: create security tests
  protected boolean isValidRequestor(String remoteUser, Configuration conf)
      throws IOException {
    if (remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to GetJournalEditServlet");
      return false;
    }

    String[] validRequestors = {
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_JOURNALNODE_USER_NAME_KEY),
            NameNode.getAddress(conf).getHostName()) };

    for (String v : validRequestors) {
      if (v != null && v.equals(remoteUser)) {
        if (LOG.isDebugEnabled())
          LOG.debug("isValidRequestor is allowing: " + remoteUser);
        return true;
      }
    }
    if (LOG.isDebugEnabled())
      LOG.debug("isValidRequestor is rejecting: " + remoteUser);
    return false;
  }
  
  private boolean checkRequestorOrSendError(Configuration conf,
      HttpServletRequest request, HttpServletResponse response)
          throws IOException {
    if (UserGroupInformation.isSecurityEnabled()
        && !isValidRequestor(request.getRemoteUser(), conf)) {
      response
          .sendError(HttpServletResponse.SC_FORBIDDEN,
              "Only Namenode and another Journal service may access this servlet");
      LOG.warn("Received non-NN/Journal request for edits from "
          + request.getRemoteHost());
      return false;
    }
    return true;
  }
  
  private boolean checkStorageInfoOrSendError(JNStorage storage,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String myStorageInfoString = storage.toColonSeparatedString();
    String theirStorageInfoString = request.getParameter(STORAGEINFO_PARAM);
    
    if (theirStorageInfoString != null
        && !myStorageInfoString.equals(theirStorageInfoString)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
              "This node has storage info " + myStorageInfoString
                  + " but the requesting node expected "
                  + theirStorageInfoString);
      LOG.warn("Received an invalid request file transfer request "
          + " with storage info " + theirStorageInfoString);
      return false;
    }
    return true;
  }
  
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final ServletContext context = getServletContext();
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      final String journalId = request.getParameter(JOURNAL_ID_PARAM);
      QuorumJournalManager.checkJournalId(journalId);
      final JNStorage storage = JournalNodeHttpServer
          .getJournalFromContext(context, journalId).getStorage();

      // Check security
      if (!checkRequestorOrSendError(conf, request, response)) {
        return;
      }

      // Check that the namespace info is correct
      if (!checkStorageInfoOrSendError(storage, request, response)) {
        return;
      }
      
      long segmentTxId = ServletUtil.parseLongParam(request,
          SEGMENT_TXID_PARAM);

      // TODO: technically we should sychronize against something here
      // because the file could get finalized in between us locating it
      // and us opening it.
      FileJournalManager fjm = storage.getJournalManager();
      File editFile;
      FileInputStream editFileIn;
      
      synchronized (fjm) {
        // Synchronize on the FJM so that the file doesn't get finalized
        // out from underneath us while we're in the process of opening
        // it up.
        EditLogFile elf = fjm.getLogFile(
            segmentTxId);
        if (elf == null) {
          response.sendError(HttpServletResponse.SC_NOT_FOUND,
              "No edit log found starting at txid " + segmentTxId);
          return;
        }
        editFile = elf.getFile();
        GetImageServlet.setVerificationHeaders(response, editFile);
        GetImageServlet.setFileNameHeaders(response, editFile);
        editFileIn = new FileInputStream(editFile);
      }
      
      DataTransferThrottler throttler = GetImageServlet.getThrottler(conf);

      // send edits
      TransferFsImage.getFileServer(response, editFile, editFileIn, throttler);

    } catch (Throwable t) {
      String errMsg = "getedit failed. " + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errMsg);
      throw new IOException(errMsg);
    }
  }
}
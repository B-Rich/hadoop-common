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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
class IPCLoggerChannel implements AsyncLogger {

  private final Configuration conf;
  private final InetSocketAddress addr;
  private QJournalProtocol proxy;

  // TODO: keep track of the amount of queued data to the
  // executor and start throwing exceptions if it's too far behind.
  private final ListeningExecutorService executor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private final String journalId;
  private final NamespaceInfo nsInfo;
  private int httpPort = -1;
  
  public IPCLoggerChannel(Configuration conf,
      NamespaceInfo nsInfo,
      String journalId,
      InetSocketAddress addr) {
    this.conf = conf;
    this.nsInfo = nsInfo;
    this.journalId = journalId;
    this.addr = addr;
    executor = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Logger channel to " + addr)
            .setUncaughtExceptionHandler(
                UncaughtExceptionHandlers.systemExit())
            .build()));
  }
  
  @Override
  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  private QJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;

    RPC.setProtocolEngine(conf,
        QJournalProtocolPB.class, ProtobufRpcEngine.class);
    QJournalProtocolPB pbproxy = RPC.getProxy(
        QJournalProtocolPB.class,
        RPC.getProtocolVersion(QJournalProtocolPB.class),
        addr, conf);
    return new QJournalProtocolTranslatorPB(pbproxy);
  }
  
  @Override
  public URL buildURLToFetchLogs(long segmentTxId) {
    Preconditions.checkArgument(segmentTxId > 0,
        "Invalid segment: %s", segmentTxId);
    Preconditions.checkState(httpPort != -1,
        "HTTP port not set yet");
        
    try {
      StringBuilder path = new StringBuilder("/getimage?");
      path.append("jid=").append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&segmentTxId=").append(segmentTxId);
      path.append("&storageinfo=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
      return new URL("http", addr.getHostName(), httpPort, path.toString());
    } catch (MalformedURLException e) {
      // should never get here.
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      // should never get here -- everyone supports UTF-8.
      throw new RuntimeException(e);
    }
  }

  private RequestInfo createReqInfo() {
    Preconditions.checkState(epoch > 0, "bad epoch: " + epoch);
    return new RequestInfo(journalId, epoch, ipcSerial++);
  }

  @Override
  public ListenableFuture<GetEpochInfoResponseProto> getEpochInfo() {
    return executor.submit(new Callable<GetEpochInfoResponseProto>() {
      @Override
      public GetEpochInfoResponseProto call() throws IOException {
        return getProxy().getEpochInfo(journalId);
      }
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final long epoch) {
    return executor.submit(new Callable<NewEpochResponseProto>() {
      @Override
      public NewEpochResponseProto call() throws IOException {
        NewEpochResponseProto ret = getProxy().newEpoch(journalId, nsInfo, epoch);
        // Fill in HTTP port. TODO: is there a more elegant place to put this?
        httpPort = ret.getHttpPort();
        return ret;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long firstTxnId, final int numTxns, final byte[] data) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().journal(createReqInfo(), firstTxnId, numTxns, data);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> startLogSegment(final long txid) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().startLogSegment(createReqInfo(), txid);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> finalizeLogSegment(
      final long startTxId, final long endTxId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().finalizeLogSegment(createReqInfo(),
            startTxId, endTxId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<GetEditLogManifestResponseProto> getEditLogManifest(
      final long fromTxnId) {
    return executor.submit(new Callable<GetEditLogManifestResponseProto>() {
      @Override
      public GetEditLogManifestResponseProto call() throws IOException {
        return getProxy().getEditLogManifest(
            journalId,
            fromTxnId);
      }
    });
  }

  @Override
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      final long segmentTxId) {
    return executor.submit(new Callable<PrepareRecoveryResponseProto>() {
      @Override
      public PrepareRecoveryResponseProto call() throws IOException {
        PrepareRecoveryResponseProto ret =
            getProxy().prepareRecovery(createReqInfo(), segmentTxId);
        httpPort = ret.getHttpPort();
        return ret;
      }
    });
  }

  @Override
  public ListenableFuture<Void> acceptRecovery(
      final RemoteEditLogProto log, final URL url) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().acceptRecovery(createReqInfo(), log, url);
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return "Channel to journal node " + addr; 
  }
}

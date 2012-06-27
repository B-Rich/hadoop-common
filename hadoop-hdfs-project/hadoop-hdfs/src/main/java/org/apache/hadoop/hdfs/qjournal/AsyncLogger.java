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

import java.net.URL;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import com.google.common.util.concurrent.ListenableFuture;

interface AsyncLogger {
  public ListenableFuture<Void> sendEdits(
      final long firstTxnId, final int numTxns, final byte[] data);

  public ListenableFuture<Void> startLogSegment(long txid);

  public ListenableFuture<GetEpochInfoResponseProto> getEpochInfo();

  public ListenableFuture<NewEpochResponseProto> newEpoch(
      long epoch);

  public ListenableFuture<Void> finalizeLogSegment(long startTxId, long endTxId);

  public ListenableFuture<GetEditLogManifestResponseProto> getEditLogManifest(
      long fromTxnId);

  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      long segmentTxId);
  
  public ListenableFuture<Void> acceptRecovery(RemoteEditLogProto log, URL fromUrl);

  public void setEpoch(long e);

  public URL buildURLToFetchLogs(long segmentTxId);
}
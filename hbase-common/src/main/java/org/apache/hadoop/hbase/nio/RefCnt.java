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
package org.apache.hadoop.hbase.nio;

import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.AbstractReferenceCounted;
import org.apache.hbase.thirdparty.io.netty.util.Recycler.Handle;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCounted;

/**
 * Maintain an reference count integer inside to track life cycle of {@link ByteBuff}, if the
 * reference count become 0, it'll call {@link Recycler#free()} exactly once.
 */
@InterfaceAudience.Private
public class RefCnt extends AbstractReferenceCounted {

  private Recycler recycler = ByteBuffAllocator.NONE;
  private final Handle<RefCnt> handle;

  private static final org.apache.hbase.thirdparty.io.netty.util.Recycler<RefCnt> RECYCLER =
      new org.apache.hbase.thirdparty.io.netty.util.Recycler<RefCnt>() {
    @Override
    protected RefCnt newObject(Handle<RefCnt> handle) {
      return new RefCnt(handle);
    }
  };

  private RefCnt(Handle<RefCnt> handle) {
    this.handle = handle;
  }

  /**
   * Create an {@link RefCnt} with an initial reference count = 1. If the reference count become
   * zero, the recycler will do nothing. Usually, an Heap {@link ByteBuff} will use this kind of
   * refCnt to track its life cycle, it help to abstract the code path although it's not really
   * needed to track on heap ByteBuff.
   */
  public static RefCnt create() {
    return create(ByteBuffAllocator.NONE);
  }

  public static RefCnt create(Recycler recycler) {
    RefCnt entry = RECYCLER.get();
    entry.setRefCnt(1);
    entry.recycler = recycler;
    return entry;
  }

  @Override
  protected final void deallocate() {
    this.recycler.free();
    handle.recycle(this);
  }

  @Override
  public final ReferenceCounted touch(Object hint) {
    throw new UnsupportedOperationException();
  }
}

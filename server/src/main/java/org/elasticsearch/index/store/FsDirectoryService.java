/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class FsDirectoryService extends DirectoryService {

    protected final IndexStore indexStore; // NOTE:htt, indexStore的机制
    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);
    public static final List<String> DEFAULT_HYBRID_EXTENSIONS = Collections.unmodifiableList(Arrays.asList("cfs")); // NOTE:htt, 默认的extensions
    public static final Setting<List<String>> INDEX_STORE_HYBRID_EXTENSIONS =
            Setting.listSetting("index.store.hybrid.extensions", DEFAULT_HYBRID_EXTENSIONS, Function.identity(),
                    Property.IndexScope, Property.NodeScope);
    private final ShardPath path;

    @Inject
    public FsDirectoryService(IndexSettings indexSettings, IndexStore indexStore, ShardPath path) {
        super(path.getShardId(), indexSettings);
        this.path = path;
        this.indexStore = indexStore;
    }

    @Override
    public Directory newDirectory() throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        Directory wrapped = newFSDirectory(location, lockFactory);
        Set<String> preLoadExtensions = new HashSet<>(
                indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        wrapped = setPreload(wrapped, location, lockFactory, preLoadExtensions);
        return wrapped;
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        final String storeType = indexSettings.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
            IndexModule.Type.FS.getSettingsKey());

        if (IndexModule.Type.HYBRIDFS.match(storeType)) {
            final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
            if (primaryDirectory instanceof MMapDirectory) { // NOTE:htt, 如果支持mmap读取文件，则采用hybrid方式
                return new HybridDirectory(location, lockFactory, primaryDirectory,indexSettings);
            } else {
                return primaryDirectory;
            }
        } else if (IndexModule.Type.FS.match(storeType)) {
            return FSDirectory.open(location, lockFactory); // use lucene defaults
        } else if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new SimpleFSDirectory(location, lockFactory);
        } else if (IndexModule.Type.NIOFS.match(storeType)) {
            return new NIOFSDirectory(location, lockFactory);
        } else if (IndexModule.Type.MMAPFS.match(storeType)) {
            return new MMapDirectory(location, lockFactory);
        }
        throw new IllegalArgumentException("No directory found for type [" + storeType + "]");
    }

    private static Directory setPreload(Directory directory, Path location, LockFactory lockFactory,
            Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false
                && directory instanceof MMapDirectory
                && ((MMapDirectory) directory).getPreload() == false) {
            if (preLoadExtensions.contains("*")) {
                ((MMapDirectory) directory).setPreload(true);
                return directory;
            }
            MMapDirectory primary = new MMapDirectory(location, lockFactory);
            primary.setPreload(true);
            return new FileSwitchDirectory(preLoadExtensions, primary, directory, true) {
                @Override
                public String[] listAll() throws IOException {
                    // avoid listing twice
                    return primary.listAll();
                }
            };
        }
        return directory;
    }

    public static class HybridDirectory extends NIOFSDirectory { // NOTE:htt, HybridDirectory支持
        private final FSDirectory randomAccessDirectory;
        protected final Set<String> hybridExtensions; // NOTE:htt, hybrids支持的拓展文件
        HybridDirectory(Path location, LockFactory lockFactory, FSDirectory randomAccessDirectory) throws IOException {
            super(location, lockFactory);
            this.randomAccessDirectory = randomAccessDirectory;
            hybridExtensions = new HashSet<>(DEFAULT_HYBRID_EXTENSIONS);
        }
        HybridDirectory(Path location, LockFactory lockFactory, FSDirectory randomAccessDirectory, IndexSettings indexSettings) throws IOException {
            super(location, lockFactory);
            this.randomAccessDirectory = randomAccessDirectory;
            hybridExtensions = new HashSet<>(indexSettings.getValue(INDEX_STORE_HYBRID_EXTENSIONS));
        }
        public Set<String> getHybridExtensions() {
            return hybridExtensions;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            String extension = FileSwitchDirectory.getExtension(name);
            // We are mmapping norms, docvalues as well as term dictionaries, all other files are served through NIOFS
            // this provides good random access performance and does not lead to page cache thrashing.
            if (hybridExtensions.contains(extension)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return randomAccessDirectory.openInput(name, context);
            } else {
                return super.openInput(name, context);
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, randomAccessDirectory);
        }

        Directory getRandomAccessDirectory() {
            return randomAccessDirectory;
        }
    }
}

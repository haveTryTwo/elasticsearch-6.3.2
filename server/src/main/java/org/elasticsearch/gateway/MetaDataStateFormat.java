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
package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * MetaDataStateFormat is a base class to write checksummed
 * XContent based files to one or more directories in a standardized directory structure.
 * @param <T> the type of the XContent base data-structure
 */
public abstract class MetaDataStateFormat<T> { // NOTE: htt, state operation for index (read/write, ../_state/state-xx.st) or cluste(../_state/global-xxx.st)  or node(../_state/node-xxx.st) or shard(../_state/state-xx.st)
    public static final XContentType FORMAT = XContentType.SMILE;
    public static final String STATE_DIR_NAME = "_state"; // NOTE: htt, _stat目录，用于将相关元信息记录到本地，包括分片信息，索引信息，节点信息，元信息, _state directory for index
    public static final String STATE_FILE_EXTENSION = ".st"; // NOTE: htt, state文件 with suffix of .st

    private static final String STATE_FILE_CODEC = "state";
    private static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 0;
    private static final int STATE_FILE_VERSION = 1;
    private static final int STATE_FILE_VERSION_ES_2X_AND_BELOW = 0;
    private static final int BUFFER_SIZE = 4096;
    private final String prefix;
    private final Pattern stateFilePattern;


    /**
     * Creates a new {@link MetaDataStateFormat} instance
     */
    protected MetaDataStateFormat(String prefix) { // NOTE: htt, index state prefix is : state-
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");

    }

    /**
     * Writes the given state to the given directories. The state is written to a
     * state directory ({@value #STATE_DIR_NAME}) underneath each of the given file locations and is created if it
     * doesn't exist. The state is serialized to a temporary file in that directory and is then atomically moved to
     * it's target filename of the pattern <tt>{prefix}{version}.st</tt>.
     *
     * @param state the state object to write
     * @param locations the locations where the state should be written to.
     * @throws IOException if an IOException occurs
     */
    public final void write(final T state, final Path... locations) throws IOException { // NOTE: htt, index : 生成 state-xx.st 元信息文件, and remove old file
        if (locations == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }
        if (locations.length <= 0) {
            throw new IllegalArgumentException("One or more locations required");
        }
        final long maxStateId = findMaxStateId(prefix, locations)+1; // NOTE: htt, stateId + 1
        assert maxStateId >= 0 : "maxStateId must be positive but was: [" + maxStateId + "]";
        final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION; // NOTE: htt, index: state-18.st, 节点: global-xx.st
        Path stateLocation = locations[0].resolve(STATE_DIR_NAME);
        Files.createDirectories(stateLocation);
        final Path tmpStatePath = stateLocation.resolve(fileName + ".tmp"); // NOTE: htt, index: ../_state/state-18.st.tmp; 节点：_state/global-xx.st.tmp
        final Path finalStatePath = stateLocation.resolve(fileName); // NOTE: htt, index: ../state/state-18.st; 节点: _state/global-xx.st
        try {
            final String resourceDesc = "MetaDataStateFormat.write(path=\"" + tmpStatePath + "\")";
            try (OutputStreamIndexOutput out =
                     new OutputStreamIndexOutput(resourceDesc, fileName, Files.newOutputStream(tmpStatePath), BUFFER_SIZE)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION); // NOTE: htt, index, 3f d7 6c 17 05 73 74 61  74 65 00 00 00 01
                out.writeInt(FORMAT.index()); // NOTE: htt, SMILE:1
                try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    } })) {

                    builder.startObject();
                    {
                        toXContent(builder, state);
                    }
                    builder.endObject();
                }
                CodecUtil.writeFooter(out); // NOTE: htt, index c0 28 93 e8 00 00 00 00  00 00 00 00 15 22 e4 a5
            }
            IOUtils.fsync(tmpStatePath, false); // fsync the state file // NOTE: htt, fsync
            Files.move(tmpStatePath, finalStatePath, StandardCopyOption.ATOMIC_MOVE);
            IOUtils.fsync(stateLocation, true);
            for (int i = 1; i < locations.length; i++) { // NOTE: htt, write ${data.path }/nodes/0/_state/node-xx.st in echo ${data.path}
                stateLocation = locations[i].resolve(STATE_DIR_NAME);
                Files.createDirectories(stateLocation);
                Path tmpPath = stateLocation.resolve(fileName + ".tmp");
                Path finalPath = stateLocation.resolve(fileName);
                try {
                    Files.copy(finalStatePath, tmpPath);
                    IOUtils.fsync(tmpPath, false); // fsync the state file
                    // we are on the same FileSystem / Partition here we can do an atomic move
                    Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
                    IOUtils.fsync(stateLocation, true);
                } finally {
                    Files.deleteIfExists(tmpPath);
                }
            }
        } finally {
            Files.deleteIfExists(tmpStatePath);
        }
        cleanupOldFiles(prefix, fileName, locations); // NOTE: htt, cleanup old state-xxx.st files
    }

    protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream ) throws IOException {
        return XContentFactory.contentBuilder(type, stream);
    }

    /**
     * Writes the given state to the given XContentBuilder
     * Subclasses need to implement this class for theirs specific state.
     */
    public abstract void toXContent(XContentBuilder builder, T state) throws IOException;

    /**
     * Reads a new instance of the state from the given XContentParser
     * Subclasses need to implement this class for theirs specific state.
     */
    public abstract T fromXContent(XContentParser parser) throws IOException;

    /**
     * Reads the state from a given file and compares the expected version against the actual version of
     * the state.
     */
    public final T read(NamedXContentRegistry namedXContentRegistry, Path file) throws IOException { // NOTE: htt, index read state-xx.st to IndexMetaData
        try (Directory dir = newDirectory(file.getParent())) {
            try (IndexInput indexInput = dir.openInput(file.getFileName().toString(), IOContext.DEFAULT)) {
                 // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput); // NOTE: htt, check foot and crc
                final int fileVersion = CodecUtil.checkHeader(indexInput, STATE_FILE_CODEC, MIN_COMPATIBLE_STATE_FILE_VERSION, // NOTE: htt, check heder
                    STATE_FILE_VERSION);
                final XContentType xContentType = XContentType.values()[indexInput.readInt()];
                if (xContentType != FORMAT) {
                    throw new IllegalStateException("expected state in " + file + " to be " + FORMAT + " format but was " + xContentType);
                }
                if (fileVersion == STATE_FILE_VERSION_ES_2X_AND_BELOW) {
                    // format version 0, wrote a version that always came from the content state file and was never used
                    indexInput.readLong(); // version currently unused
                }
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                try (IndexInput slice = indexInput.slice("state_xcontent", filePointer, contentSize)) {
                    try (XContentParser parser = XContentFactory.xContent(FORMAT)
                            .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                new InputStreamIndexInput(slice, contentSize))) {
                        return fromXContent(parser);
                    }
                }
            } catch(CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                // we trick this into a dedicated exception with the original stacktrace
                throw new CorruptStateException(ex);
            }
        }
    }

    protected Directory newDirectory(Path dir) throws IOException {
        return new SimpleFSDirectory(dir);
    }

    private void cleanupOldFiles(final String prefix, final String currentStateFile, Path[] locations) throws IOException {
        final DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                final String entryFileName = entry.getFileName().toString();
                return Files.isRegularFile(entry)
                        && entryFileName.startsWith(prefix) // only state files // NOTE: htt, state-
                        && currentStateFile.equals(entryFileName) == false; // keep the current state file around  // NOTE: htt, not equal to current number
            }
        };
        // now clean up the old files
        for (Path dataLocation : locations) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataLocation.resolve(STATE_DIR_NAME), filter)) {
                for (Path stateFile : stream) {
                    Files.deleteIfExists(stateFile);
                }
            }
        }
    }

    long findMaxStateId(final String prefix, Path... locations) throws IOException {
        long maxId = -1;
        for (Path dataLocation : locations) {
            final Path resolve = dataLocation.resolve(STATE_DIR_NAME);
            if (Files.exists(resolve)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolve, prefix + "*")) {
                    for (Path stateFile : stream) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long id = Long.parseLong(matcher.group(1));
                            maxId = Math.max(maxId, id);
                        }
                    }
                }
            }
        }
        return maxId;
    }

    /**
     * Tries to load the latest state from the given data-locations. It tries to load the latest state determined by
     * the states version from one or more data directories and if none of the latest states can be loaded an exception
     * is thrown to prevent accidentally loading a previous state and silently omitting the latest state.
     *
     * @param logger a logger instance
     * @param dataLocations the data-locations to try.
     * @return the latest state or <code>null</code> if no state was found.
     */
    public  T loadLatestState(Logger logger, NamedXContentRegistry namedXContentRegistry, Path... dataLocations) throws IOException {
        List<PathAndStateId> files = new ArrayList<>();
        long maxStateId = -1;
        boolean maxStateIdIsLegacy = true;
        if (dataLocations != null) { // select all eligible files first
            for (Path dataLocation : dataLocations) {
                final Path stateDir = dataLocation.resolve(STATE_DIR_NAME);  // NOTE: htt, ${data.path}/nodes/0/_state(节点级别_state), 还有索引级别元信息, ${data.paths}/nodes/0/indices/${index.UUID}/_state
                // now, iterate over the current versions, and find latest one
                // we don't check if the stateDir is present since it could be deleted
                // after the check. Also if there is a _state file and it's not a dir something is really wrong
                // we don't pass a glob since we need the group part for parsing
                try (DirectoryStream<Path> paths = Files.newDirectoryStream(stateDir)) {
                    for (Path stateFile : paths) { // NOTE: htt, index: state-19.st
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString()); // NOTE: htt, prefix should be set such as node- to get current .st file
                        if (matcher.matches()) {
                            final long stateId = Long.parseLong(matcher.group(1)); // NOTE: htt, 19
                            maxStateId = Math.max(maxStateId, stateId);
                            final boolean legacy = MetaDataStateFormat.STATE_FILE_EXTENSION.equals(matcher.group(2)) == false;
                            maxStateIdIsLegacy &= legacy; // on purpose, see NOTE below
                            PathAndStateId pav = new PathAndStateId(stateFile, stateId, legacy);
                            logger.trace("found state file: {}", pav);
                            files.add(pav);
                        }
                    }
                } catch (NoSuchFileException | FileNotFoundException ex) {
                    // no _state directory -- move on
                }
            }
        }
        final List<Throwable> exceptions = new ArrayList<>();
        T state = null;
        // NOTE: we might have multiple version of the latest state if there are multiple data dirs.. for this case
        //       we iterate only over the ones with the max version. If we have at least one state file that uses the
        //       new format (ie. legacy == false) then we know that the latest version state ought to use this new format.
        //       In case the state file with the latest version does not use the new format while older state files do,
        //       the list below will be empty and loading the state will fail
        Collection<PathAndStateId> pathAndStateIds = files
                .stream()
                .filter(new StateIdAndLegacyPredicate(maxStateId, maxStateIdIsLegacy)) // NOTE: htt, get the newest state-xx.st file
                .collect(Collectors.toCollection(ArrayList::new));

        for (PathAndStateId pathAndStateId : pathAndStateIds) { // NOTE: htt, get the newest state-xx.st file
            try {
                final Path stateFile = pathAndStateId.file;
                final long id = pathAndStateId.id;
                if (pathAndStateId.legacy) { // read the legacy format -- plain XContent
                    final byte[] data = Files.readAllBytes(stateFile);
                    if (data.length == 0) {
                        logger.debug("{}: no data for [{}], ignoring...", prefix, stateFile.toAbsolutePath());
                        continue;
                    }
                    try (XContentParser parser = XContentHelper
                            .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, new BytesArray(data))) {
                        state = fromXContent(parser);
                    }
                    if (state == null) {
                        logger.debug("{}: no data for [{}], ignoring...", prefix, stateFile.toAbsolutePath());
                    }
                } else {
                    state = read(namedXContentRegistry, stateFile);
                    logger.trace("state id [{}] read from [{}]", id, stateFile.getFileName());
                }
                return state; // NOTE:htt, 读取到一个之后，就直接返回
            } catch (Exception e) {
                exceptions.add(new IOException("failed to read " + pathAndStateId.toString(), e));
                logger.debug(() -> new ParameterizedMessage(
                        "{}: failed to read [{}], ignoring...", pathAndStateId.file.toAbsolutePath(), prefix), e);
            }
        }
        // if we reach this something went wrong
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (files.size() > 0) {
            // We have some state files but none of them gave us a usable state
            throw new IllegalStateException("Could not find a state file to recover from among " + files);
        }
        return state;
    }

    /**
     * Filters out all {@link org.elasticsearch.gateway.MetaDataStateFormat.PathAndStateId} instances with a different id than
     * the given one.
     */
    private static final class StateIdAndLegacyPredicate implements Predicate<PathAndStateId> { // NOTE: htt, test PathAndStateId
        private final long id;
        private final boolean legacy;

        StateIdAndLegacyPredicate(long id, boolean legacy) {
            this.id = id;
            this.legacy = legacy;
        }

        @Override
        public boolean test(PathAndStateId input) {
            return input.id == id && input.legacy == legacy;
        }
    }

    /**
     * Internal struct-like class that holds the parsed state id, the file
     * and a flag if the file is a legacy state ie. pre 1.5
     */
    private static class PathAndStateId { // NOTE: htt, path and stateId
        final Path file;
        final long id;
        final boolean legacy;

        private PathAndStateId(Path file, long id, boolean legacy) {
            this.file = file;
            this.id = id;
            this.legacy = legacy;
        }

        @Override
        public String toString() {
            return "[id:" + id + ", legacy:" + legacy + ", file:" + file.toAbsolutePath() + "]";
        }
    }

    /**
     * Deletes all meta state directories recursively for the given data locations
     * @param dataLocations the data location to delete
     */
    public static void deleteMetaState(Path... dataLocations) throws IOException {
        Path[] stateDirectories = new Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            stateDirectories[i] = dataLocations[i].resolve(STATE_DIR_NAME);
        }
        IOUtils.rm(stateDirectories);
    }
}

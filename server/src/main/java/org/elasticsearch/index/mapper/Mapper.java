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

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> { // NOTE: htt, mapper which has name, merge() and update field type()

    public static class BuilderContext { // NOTE: htt, builder context
        private final Settings indexSettings;
        private final ContentPath contentPath;

        public BuilderContext(Settings indexSettings, ContentPath contentPath) {
            Objects.requireNonNull(indexSettings, "indexSettings is required");
            this.contentPath = contentPath;
            this.indexSettings = indexSettings;
        }

        public ContentPath path() {
            return this.contentPath;
        }

        public Settings indexSettings() {
            return this.indexSettings;
        }

        public Version indexCreatedVersion() {
            return Version.indexCreated(indexSettings);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends Mapper> { // NOTE: htt, builder a mapper

        public String name;

        protected T builder;

        protected Builder(String name) {
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        /** Returns a newly built mapper. */
        public abstract Y build(BuilderContext context);
    }

    public interface TypeParser { // NOTE: htt, typer parser

        class ParserContext { // NOTE: htt, parse context include index analyzer and similarity, mapper service

            private final String type; // NOTE: htt, 类型，6.x之后支持一种，默认为_doc

            private final IndexAnalyzers indexAnalyzers; // NOTE: htt, 索引的分析器

            private final Function<String, SimilarityProvider> similarityLookupService; // NOTE: htt, 相关度算法，包括BM25，TF/IDF，boolean

            private final MapperService mapperService;

            private final Function<String, TypeParser> typeParsers;

            private final Version indexVersionCreated; // NOTE: htt, 创建的索引版本

            private final Supplier<QueryShardContext> queryShardContextSupplier; // NOTE: htt, 指定索引下的shard查询的上下文

            public ParserContext(String type, IndexAnalyzers indexAnalyzers, Function<String, SimilarityProvider> similarityLookupService,
                                 MapperService mapperService, Function<String, TypeParser> typeParsers,
                                 Version indexVersionCreated, Supplier<QueryShardContext> queryShardContextSupplier) {
                this.type = type;
                this.indexAnalyzers = indexAnalyzers;
                this.similarityLookupService = similarityLookupService;
                this.mapperService = mapperService;
                this.typeParsers = typeParsers;
                this.indexVersionCreated = indexVersionCreated;
                this.queryShardContextSupplier = queryShardContextSupplier;
            }

            public String type() {
                return type;
            }

            public IndexAnalyzers getIndexAnalyzers() {
                return indexAnalyzers;
            }

            public SimilarityProvider getSimilarity(String name) {
                return similarityLookupService.apply(name);
            }

            public MapperService mapperService() {
                return mapperService;
            }

            public TypeParser typeParser(String type) {
                return typeParsers.apply(type);
            }

            public Version indexVersionCreated() {
                return indexVersionCreated;
            }

            public Supplier<QueryShardContext> queryShardContextSupplier() {
                return queryShardContextSupplier;
            }

            public boolean isWithinMultiField() { return false; }

            protected Function<String, TypeParser> typeParsers() { return typeParsers; }

            protected Function<String, SimilarityProvider> similarityLookupService() { return similarityLookupService; }

            public ParserContext createMultiFieldContext(ParserContext in) {
                return new MultiFieldParserContext(in) {
                    @Override
                    public boolean isWithinMultiField() { return true; }
                };
            }

            static class MultiFieldParserContext extends ParserContext {
                MultiFieldParserContext(ParserContext in) {
                    super(in.type(), in.indexAnalyzers, in.similarityLookupService(), in.mapperService(), in.typeParsers(),
                            in.indexVersionCreated(), in.queryShardContextSupplier());
                }
            }

        }
        // NOTE: htt, node为字段下属性以及对应的值，如 doc_values: true
        Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;
    }

    private final String simpleName; // NOTE: htt, mapper的简单名称

    public Mapper(String simpleName) {
        Objects.requireNonNull(simpleName);
        this.simpleName = simpleName;
    }

    /** Returns the simple name, which identifies this mapper against other mappers at the same level in the mappers hierarchy
     * TODO: make this protected once Mapper and FieldMapper are merged together */
    public final String simpleName() {
        return simpleName;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String name();

    /** Return the merge of {@code mergeWith} into this.
     *  Both {@code this} and {@code mergeWith} will be left unmodified. */
    public abstract Mapper merge(Mapper mergeWith, boolean updateAllTypes);

    /**
     * Update the field type of this mapper. This is necessary because some mapping updates
     * can modify mappings across several types. This method must return a copy of the mapper
     * so that the current mapper is not modified.
     */
    public abstract Mapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType);
}

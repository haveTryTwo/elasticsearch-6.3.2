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

package org.elasticsearch.common;

/**
 * Utility class for glob-like matching
 */
public class Glob { // NOTE:htt, 模式匹配

    /**
     * Match a String against the given pattern, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     *
     * @param pattern the pattern to match against
     * @param str     the String to match
     * @return whether the String matches the given pattern
     */
    public static boolean globMatch(String pattern, String str) { // NOTE:htt, 查找简单的字符串模式匹配
        if (pattern == null || str == null) {
            return false;
        }
        int firstIndex = pattern.indexOf('*');  // NOTE:htt, 查找*符号
        if (firstIndex == -1) {
            return pattern.equals(str); // NOTE:htt, 没找到则字符串匹配
        }
        if (firstIndex == 0) { // NOTE:htt, 第一个字符为*
            if (pattern.length() == 1) { // NOTE:htt, 仅一个*则返回true
                return true;
            }
            int nextIndex = pattern.indexOf('*', firstIndex + 1);
            if (nextIndex == -1) {
                return str.endsWith(pattern.substring(1)); // NOTE:htt, 如果仅一个*，则比较str是否和pattern剩余相等
            } else if (nextIndex == 1) {
                // Double wildcard "**" - skipping the first "*"
                return globMatch(pattern.substring(1), str); // NOTE:htt, 如果两个**，则直接忽略一个继续比较
            }
            String part = pattern.substring(1, nextIndex);
            int partIndex = str.indexOf(part);
            while (partIndex != -1) {
                if (globMatch(pattern.substring(nextIndex), str.substring(partIndex + part.length()))) { // NOTE:htt, 查找后续内容
                    return true;
                }
                partIndex = str.indexOf(part, partIndex + 1); // NOTE:htt, 从下一个位置继续查找part，如果找到则循环找后续内容
            }
            return false;
        }
        return (str.length() >= firstIndex &&
            pattern.substring(0, firstIndex).equals(str.substring(0, firstIndex)) &&
            globMatch(pattern.substring(firstIndex), str.substring(firstIndex))); // NOTE:htt, *前面内容相同并继续比较*之后内容
    }

}

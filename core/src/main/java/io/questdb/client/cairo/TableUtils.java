/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.cairo;

public final class TableUtils {
    public static boolean isValidColumnName(CharSequence columnName, int fsFileNameLimit) {
        final int length = columnName.length();
        if (length > fsFileNameLimit) {
            // Most file systems do not support file names longer than 255 bytes
            return false;
        }

        for (int i = 0; i < length; i++) {
            char c = columnName.charAt(i);
            switch (c) {
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case '\u0000': // Control characters
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '	':
                case '\u000B':
                case '\u000c':
                case '\n':
                case '\r':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
                default:
                    break;
            }
        }
        return length > 0;
    }

    public static boolean isValidTableName(CharSequence tableName, int fsFileNameLimit) {
        final int length = tableName.length();
        if (length > fsFileNameLimit) {
            // Most file systems do not support file names longer than 255 bytes
            return false;
        }

        for (int i = 0; i < length; i++) {
            char c = tableName.charAt(i);
            switch (c) {
                case '.':
                    if (i == 0 || i == length - 1 || tableName.charAt(i - 1) == '.') {
                        // Single dot in the middle is allowed only
                        // Starting from . hides directory in Linux
                        // Ending . can be trimmed by some Windows versions / file systems
                        // Double, triple dot look suspicious
                        // Single dot allowed as compatibility,
                        // when someone uploads 'file_name.csv' the file name used as the table name
                        return false;
                    }
                    break;
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\u0000':  // Control characters
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '	':
                case '\u000B':
                case '\u000c':
                case '\r':
                case '\n':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
            }
        }
        return length > 0 && tableName.charAt(0) != ' ' && tableName.charAt(length - 1) != ' ';
    }
}
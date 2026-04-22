/*+*****************************************************************************
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

package io.questdb.client.test.std.str;

import io.questdb.client.cairo.CairoException;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.DirectUtf8String;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf16Sink;
import io.questdb.client.std.str.Utf8String;
import io.questdb.client.std.str.Utf8StringSink;
import io.questdb.client.std.str.Utf8s;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class Utf8sTest {

    @Test
    public void tesConvertInvalidUTF8UTF16Exception() {
        byte[] data = new byte[]{
                79, 112, 116, 46, 80, 118, 80, 110, 108, 32, 115, 116, 97, 114, 116, 95, 116, 105, 109, 101,
                61, 49, 55, 53, 55, 53, 54, 56, 54, 48, 48, 48, 48, 48, 48, 48, 48, 116, 44, 100,
                117, 114, 97, 116, 105, 111, 110, 95, 109, 115, 61, 61, 16, 0, 0, 0, 0, 64, 119,
                43, 65, 44, 112, 110, 108, 95, 105, 100, 61, 34, 105, 110, 118, 101, 115, 116, 109, 101, 110,
                116, 115, 45, 101, 118, 97, 124, 99, 97, 108, 97, 100, 97, 110, 95, 116, 105, 97, 95, 117,
                115, 100, 116, 95, 99, 95, 51, 46, 56, 53, 95, 50, 48, 50, 53, 48, 57, 48, 56, 34,
                44, 108, 101, 103, 95, 105, 100, 120, 61, 61, 16, 0, 0, 0, 0, 0, (byte) 132, (byte) 146, 64, 44,
                98, 97, 115, 101, 95, 105, 100, 120, 61, 61, 16, 0, 0, 0, 0, 0, 0, 36, 64, 44,
                112, 97, 114, 101, 110, 116, 95, 105, 100, 61, 61, 16, 0, 0, 0, 0, 0, 0, (byte) 240, (byte) 191,
                44, 116, 121, 112, 101, 61, 61, 16, 0, 0, 0, 0, 0, 0, (byte) 240, 63, 44, 110, 116, 108,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 118, 103, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, (byte) 248, 127, 44, 109, 97, 114, 107, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 109, 111, 100, 101, 108, 95, 117, 115, 100,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118, 95, 117, 115, 100, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118, 95, 109, 97, 114, 107, 61, 61, 16,
                0, 0, 0, 0, 0, 0, 0, 0, 44, 117, 110, 114, 101, 97, 108, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 117, 110, 114, 101, 97, 108, 95, 109, 97,
                114, 107, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 105, 118, 61, 61, 16, 0,
                0, 0, 0, 0, 0, 0, 0, 44, 102, 61, 61, 16, 83, 89, (byte) 235, (byte) 246, (byte) 131, 52, (byte) 252, 63,
                44, 114, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 116, 109, 61, 61, 16,
                20, 82, (byte) 235, (byte) 135, 55, 88, (byte) 228, 63, 44, 116, 116, 109, 61, 61, 16, 11, 84, (byte) 241, 120, 45,
                3, 97, 62, 44, 116, 104, 101, 116, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0,
                44, 100, 101, 108, 116, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 100, 101,
                108, 116, 97, 95, 116, 49, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 103, 97,
                109, 109, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 104, 111, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 101, 112, 115, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 118, 101, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0,
                44, 118, 97, 110, 110, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 118, 111,
                108, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 116, 109, 103, 97,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 119, 118, 101, 103, 97, 61, 61, 16,
                0, 0, 0, 0, 0, 0, 0, 0, 44, 119, 118, 111, 108, 103, 97, 61, 61, 16, 0, 0,
                0, 0, 0, 0, 0, 0, 44, 119, 118, 97, 110, 110, 97, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 119, 97, 116, 109, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0,
                0, 0, 44, 116, 104, 101, 116, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 100, 101, 108, 116, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 97, 116, 109, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 114, 101, 103, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 98, 117, 102, 103, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 102, 119, 100, 114, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 102, 117, 110, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 118, 111, 108, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 116, 114, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0, 0,
                0, 0, 44, 109, 97, 114, 107, 95, 100, 105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 97, 99, 99, 111, 117, 110, 116, 61, 34, 105, 110, 118, 101, 115, 116, 109,
                101, 110, 116, 115, 45, 101, 118, 97, 34, 44, 105, 110, 115, 116, 114, 117, 109, 101, 110, 116,
                61, 34, 99, 97, 108, 97, 100, 97, 110, 95, 116, 105, 97, 95, 117, 115, 100, 116, 95, 99,
                95, 51, 46, 56, 53, 95, 50, 48, 50, 53, 48, 57, 48, 56, 34, 44, 101, 120, 112, 61,
                34, 50, 48, 50, 53, 48, 57, 48, 56, 34, 44, 98, 117, 99, 107, 101, 116, 61, 34, 50,
                48, 50, 53, 48, 57, 48, 56, 34, 44, 98, 97, 115, 101, 61, 34, 116, 105, 97, 34, 44,
                112, 111, 114, 116, 102, 111, 108, 105, 111, 61, 34, 105, 110, 118, 101, 115, 116, 109, 101, 110,
                116, 115, 45, 101, 118, 97, 34, 44, 115, 116, 97, 114, 116, 95, 100, 105, 102, 102, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 115, 116, 97, 114, 116, 95, 112, 118, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 116, 114, 97, 99, 107, 105, 110, 103, 95, 100,
                105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 116, 114, 97, 99, 107,
                101, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118,
                95, 100, 105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101, 115,
                105, 100, 117, 97, 108, 115, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101,
                103, 97, 50, 53, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101, 103, 97,
                49, 48, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 98, 117, 102, 103, 97, 50,
                53, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 98, 117, 102, 103, 97, 49, 48,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 107, 101, 121, 61, 34, 105, 110, 118,
                101, 115, 116, 109, 101, 110, 116, 115, 45, 101, 118, 97, 124, 99, 97, 108, 97, 100, 97, 110,
                95, 116, 105, 97, 95, 117, 115, 100, 116, 95, 99, 95, 51, 46, 56, 53, 95, 50, 48, 50,
                53, 48, 57, 48, 56, 34, 32, 49, 55, 53, 55, 53, 54, 56, 54, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 10
        };
        final int len = data.length;
        long mem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putByte(mem + i, data[i]);
        }
        try {
            Utf8s.stringFromUtf8Bytes(mem, mem + len);
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "cannot convert invalid UTF-8 sequence " +
                    "to UTF-16 [seq=Opt.PvPnl start_time=1757568600000000t,duration_ms==\\x10\\x00\\x00\\x00\\x00@w+A," +
                    "pnl_id=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\",leg_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x84\\x92@," +
                    "base_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x00$@,parent_id==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0\\xBF," +
                    "type==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0?,ntl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "avg_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF8\\x7F,mark_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "model_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "pv_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,unreal_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "unreal_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,iv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "f==\\x10SY\\xEB\\xF6\\x834\\xFC?,r==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atm==\\x10\\x14R\\xEB\\x877X\\xE4?," +
                    "ttm==\\x10\\x0BT\\xF1x-\\x03a>,theta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "delta_t1==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,gamma==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "rho==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,eps==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "vega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "volga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "wvega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,wvolga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "wvanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,watmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "theta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "atm_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "bufga_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,fwdr_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "fund_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vol_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "trd_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,mark_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "account=\"investments-eva\",instrument=\"caladan_tia_usdt_c_3.85_20250908\",exp=\"20250908\",bucket=\"20250908\"," +
                    "base=\"tia\",portfolio=\"investments-eva\",start_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "start_pv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,tracking_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "tracked_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "residuals==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "rega10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,bufga25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "bufga10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,key=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\" 1757568600000000000\\x0A]");

            try {
                DirectUtf8String sequence = new DirectUtf8String().of(mem, mem + len);
                Utf8s.stringFromUtf8Bytes(sequence);
                Assert.fail();
            } catch (CairoException ex1) {
                TestUtils.assertContains(ex1.getFlyweightMessage(), "cannot convert invalid UTF-8 sequence " +
                        "to UTF-16 [seq=Opt.PvPnl start_time=1757568600000000t,duration_ms==\\x10\\x00\\x00\\x00\\x00@w+A," +
                        "pnl_id=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\",leg_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x84\\x92@," +
                        "base_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x00$@,parent_id==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0\\xBF," +
                        "type==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0?,ntl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "avg_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF8\\x7F,mark_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "model_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "pv_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,unreal_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "unreal_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,iv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "f==\\x10SY\\xEB\\xF6\\x834\\xFC?,r==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atm==\\x10\\x14R\\xEB\\x877X\\xE4?," +
                        "ttm==\\x10\\x0BT\\xF1x-\\x03a>,theta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "delta_t1==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,gamma==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "rho==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,eps==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "vega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "volga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "wvega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,wvolga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "wvanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,watmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "theta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "atm_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "bufga_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,fwdr_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "fund_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vol_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "trd_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,mark_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "account=\"investments-eva\",instrument=\"caladan_tia_usdt_c_3.85_20250908\",exp=\"20250908\",bucket=\"20250908\"," +
                        "base=\"tia\",portfolio=\"investments-eva\",start_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "start_pv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,tracking_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "tracked_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "residuals==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "rega10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,bufga25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "bufga10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,key=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\" 1757568600000000000\\x0A]");
            }
        } finally {
            Unsafe.free(mem, data.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testEqualsNcAscii() {
        final Utf8String str = new Utf8String("test1");

        Assert.assertTrue(Utf8s.equalsNcAscii("test1", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("a_longer_string", str));

        Assert.assertFalse(Utf8s.equalsNcAscii("test1", null));
    }

    @Test
    public void testPutSafeValid() {
        final Utf8StringSink sink = new Utf8StringSink();
        final String[] testStrs = {
                "abc",
                "čćžšđ",
                "ČĆŽŠĐ",
                "фубар",
                "ФУБАР",
                "你好世界",
        };
        final long buf = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            for (String testStr : testStrs) {
                byte[] bytes = testStr.getBytes(StandardCharsets.UTF_8);
                long hi = copyBytes(buf, bytes);
                sink.clear();
                Utf8s.putSafe(buf, hi, sink);
                String actual = sink.toString();
                Assert.assertEquals(testStr, actual);
            }
        } finally {
            Unsafe.free(buf, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"file.csv\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testStrCpyUtf8() {
        final int bufSize = 64;
        long mem = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            fill(mem, bufSize, (byte) 0x7F);

            Assert.assertEquals(0, Utf8s.strCpyUtf8("", mem, bufSize));
            assertUntouched(mem, bufSize, (byte) 0x7F);

            Assert.assertEquals(0, Utf8s.strCpyUtf8("hello", mem, 0));
            assertUntouched(mem, bufSize, (byte) 0x7F);

            assertStrCpyUtf8(mem, bufSize, "hello", bufSize, 5, "hello");
            assertStrCpyUtf8(mem, bufSize, "éü", bufSize, 4, "éü");
            assertStrCpyUtf8(mem, bufSize, "世界", bufSize, 6, "世界");
            assertStrCpyUtf8(mem, bufSize, "\uD83D\uDE00", bufSize, 4, "\uD83D\uDE00");
            assertStrCpyUtf8(mem, bufSize, "Aé世\uD83D\uDE00", bufSize, 10, "Aé世\uD83D\uDE00");
        } finally {
            Unsafe.free(mem, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpyUtf8FromOffset() {
        final int bufSize = 64;
        long mem = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final byte sentinel = 0x5A;
            String value = "ascii-prefixé世\uD83D\uDE00\uD800x";
            int lo = "ascii-prefix".length();
            int expectedBytes = Utf8s.utf8Bytes(value, lo, value.length());

            fill(mem, bufSize, sentinel);
            Assert.assertEquals(expectedBytes, Utf8s.strCpyUtf8(value, lo, mem, expectedBytes));
            Assert.assertEquals("é世\uD83D\uDE00?x", readUtf8(mem, expectedBytes));
            for (int i = expectedBytes; i < bufSize; i++) {
                Assert.assertEquals("write past copied bytes at offset " + i, sentinel, Unsafe.getUnsafe().getByte(mem + i));
            }
        } finally {
            Unsafe.free(mem, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpyUtf8DoesNotSplitCharactersAtLimit() {
        final int bufSize = 16;
        long mem = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            assertStrCpyUtf8(mem, bufSize, "abcdef", 3, 3, "abc");
            assertStrCpyUtf8(mem, bufSize, "aéb", 2, 1, "a");
            assertStrCpyUtf8(mem, bufSize, "a世b", 3, 1, "a");
            assertStrCpyUtf8(mem, bufSize, "a\uD83D\uDE00b", 4, 1, "a");

            assertStrCpyUtf8(mem, bufSize, "aé", 3, 3, "aé");
            assertStrCpyUtf8(mem, bufSize, "a世", 4, 4, "a世");
            assertStrCpyUtf8(mem, bufSize, "a\uD83D\uDE00", 5, 5, "a\uD83D\uDE00");
        } finally {
            Unsafe.free(mem, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpyUtf8ReplacesInvalidSurrogates() {
        final int bufSize = 16;
        long mem = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            assertStrCpyUtf8(mem, bufSize, "\uD83Da", bufSize, 2, "?a");
            assertStrCpyUtf8(mem, bufSize, "\uDE00b", bufSize, 2, "?b");
            assertStrCpyUtf8(mem, bufSize, "\uD83D", bufSize, 1, "?");
            assertStrCpyUtf8(mem, bufSize, "\uDE00", bufSize, 1, "?");
            assertStrCpyUtf8(mem, bufSize, "\uD83Da", 1, 1, "?");
            assertStrCpyUtf8(mem, bufSize, "a\uD83D", 1, 1, "a");
            assertStrCpyUtf8(mem, bufSize, "a\uDE00", 1, 1, "a");
        } finally {
            Unsafe.free(mem, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8Bytes() {
        Assert.assertEquals(0, Utf8s.utf8Bytes(""));
        Assert.assertEquals(5, Utf8s.utf8Bytes("hello"));
        Assert.assertEquals(4, Utf8s.utf8Bytes("éü"));
        Assert.assertEquals(6, Utf8s.utf8Bytes("世界"));
        Assert.assertEquals(4, Utf8s.utf8Bytes("\uD83D\uDE00"));
        Assert.assertEquals(10, Utf8s.utf8Bytes("Aé世\uD83D\uDE00"));
        Assert.assertEquals(2, Utf8s.utf8Bytes("\uD83Da"));
        Assert.assertEquals(2, Utf8s.utf8Bytes("\uDE00b"));
        Assert.assertEquals(1, Utf8s.utf8Bytes("\uD83D"));
        Assert.assertEquals(1, Utf8s.utf8Bytes("\uDE00"));
    }

    @Test
    public void testUtf8BytesRange() {
        String value = "xxAé世\uD83D\uDE00yy";
        Assert.assertEquals(10, Utf8s.utf8Bytes(value, 2, 7));
        Assert.assertEquals(1, Utf8s.utf8Bytes(value, 5, 6));
        Assert.assertEquals(4, Utf8s.utf8Bytes(value, 5, 7));
    }

    @Test
    public void testUtf8BytesWithLimit() {
        Assert.assertEquals(0, Utf8s.utf8Bytes("hello", 0));
        Assert.assertEquals(5, Utf8s.utf8Bytes("hello", 10));
        Assert.assertEquals(3, Utf8s.utf8Bytes("hello", 3));

        Assert.assertEquals(1, Utf8s.utf8Bytes("aéb", 2));
        Assert.assertEquals(3, Utf8s.utf8Bytes("aé", 3));

        Assert.assertEquals(1, Utf8s.utf8Bytes("a世b", 3));
        Assert.assertEquals(4, Utf8s.utf8Bytes("a世", 4));

        Assert.assertEquals(1, Utf8s.utf8Bytes("a\uD83D\uDE00b", 4));
        Assert.assertEquals(5, Utf8s.utf8Bytes("a\uD83D\uDE00", 5));

        Assert.assertEquals(1, Utf8s.utf8Bytes("\uD83Da", 1));
        Assert.assertEquals(1, Utf8s.utf8Bytes("\uDE00b", 1));
        Assert.assertEquals(1, Utf8s.utf8Bytes("a\uD83D", 1));
        Assert.assertEquals(1, Utf8s.utf8Bytes("a\uDE00", 1));
    }

    @Test
    public void testUtf8Support() {
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] bytes = in.getBytes(Files.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            Utf16Sink b = new StringSink();
            Utf8s.utf8ToUtf16(p, p + bytes.length, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertStrCpyUtf8(long mem, int bufSize, String value, int maxBytes, int expectedBytes, String expected) {
        final byte sentinel = 0x5A;
        fill(mem, bufSize, sentinel);

        int n = Utf8s.strCpyUtf8(value, mem, maxBytes);
        Assert.assertEquals(expectedBytes, n);
        Assert.assertEquals(expected, readUtf8(mem, n));
        for (int i = n; i < bufSize; i++) {
            Assert.assertEquals("write past copied bytes at offset " + i, sentinel, Unsafe.getUnsafe().getByte(mem + i));
        }
    }

    private static long copyBytes(long buf, byte[] bytes) {
        for (int n = bytes.length, i = 0; i < n; i++) {
            Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
        }
        return buf + bytes.length;
    }

    private static void assertUntouched(long mem, int bufSize, byte expected) {
        for (int i = 0; i < bufSize; i++) {
            Assert.assertEquals("unexpected write at offset " + i, expected, Unsafe.getUnsafe().getByte(mem + i));
        }
    }

    private static void fill(long mem, int len, byte value) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(mem + i, value);
        }
    }

    private static String readUtf8(long mem, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(mem + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private boolean copyToSinkWithTextUtil(StringSink query, String text) {
        byte[] bytes = text.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }

        boolean res = Utf8s.utf8ToUtf16(ptr, ptr + bytes.length, query);
        Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        return res;
    }
}

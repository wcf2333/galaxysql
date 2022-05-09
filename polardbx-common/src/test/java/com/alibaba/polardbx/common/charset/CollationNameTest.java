/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.charset;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static com.alibaba.polardbx.common.charset.CollationName.*;

public class CollationNameTest {
    @Test
    public void testOfString() {
        doTestOf("big5_chinese_ci", BIG5_CHINESE_CI);
        doTestOf("latin2_czech_cs", LATIN2_CZECH_CS);
        doTestOf("dec8_swedish_ci", DEC8_SWEDISH_CI);
        doTestOf("cp850_general_ci", CP850_GENERAL_CI);
        doTestOf("latin1_german1_ci", LATIN1_GERMAN1_CI);
        doTestOf("hp8_english_ci", HP8_ENGLISH_CI);
        doTestOf("koi8r_general_ci", KOI8R_GENERAL_CI);
        doTestOf("latin1_swedish_ci", LATIN1_SWEDISH_CI);
        doTestOf("latin2_general_ci", LATIN2_GENERAL_CI);
        doTestOf("swe7_swedish_ci", SWE7_SWEDISH_CI);
        doTestOf("ascii_general_ci", ASCII_GENERAL_CI);
        doTestOf("ujis_japanese_ci", UJIS_JAPANESE_CI);
        doTestOf("sjis_japanese_ci", SJIS_JAPANESE_CI);
        doTestOf("cp1251_bulgarian_ci", CP1251_BULGARIAN_CI);
        doTestOf("latin1_danish_ci", LATIN1_DANISH_CI);
        doTestOf("hebrew_general_ci", HEBREW_GENERAL_CI);
        doTestOf("tis620_thai_ci", TIS620_THAI_CI);
        doTestOf("euckr_korean_ci", EUCKR_KOREAN_CI);
        doTestOf("latin7_estonian_cs", LATIN7_ESTONIAN_CS);
        doTestOf("latin2_hungarian_ci", LATIN2_HUNGARIAN_CI);
        doTestOf("koi8u_general_ci", KOI8U_GENERAL_CI);
        doTestOf("cp1251_ukrainian_ci", CP1251_UKRAINIAN_CI);
        doTestOf("gb2312_chinese_ci", GB2312_CHINESE_CI);
        doTestOf("greek_general_ci", GREEK_GENERAL_CI);
        doTestOf("cp1250_general_ci", CP1250_GENERAL_CI);
        doTestOf("latin2_croatian_ci", LATIN2_CROATIAN_CI);
        doTestOf("gbk_chinese_ci", GBK_CHINESE_CI);
        doTestOf("cp1257_lithuanian_ci", CP1257_LITHUANIAN_CI);
        doTestOf("latin5_turkish_ci", LATIN5_TURKISH_CI);
        doTestOf("latin1_german2_ci", LATIN1_GERMAN2_CI);
        doTestOf("armscii8_general_ci", ARMSCII8_GENERAL_CI);
        doTestOf("utf8_general_ci", UTF8_GENERAL_CI);
        doTestOf("cp1250_czech_cs", CP1250_CZECH_CS);
        doTestOf("ucs2_general_ci", UCS2_GENERAL_CI);
        doTestOf("cp866_general_ci", CP866_GENERAL_CI);
        doTestOf("keybcs2_general_ci", KEYBCS2_GENERAL_CI);
        doTestOf("macce_general_ci", MACCE_GENERAL_CI);
        doTestOf("macroman_general_ci", MACROMAN_GENERAL_CI);
        doTestOf("cp852_general_ci", CP852_GENERAL_CI);
        doTestOf("latin7_general_ci", LATIN7_GENERAL_CI);
        doTestOf("latin7_general_cs", LATIN7_GENERAL_CS);
        doTestOf("macce_bin", MACCE_BIN);
        doTestOf("cp1250_croatian_ci", CP1250_CROATIAN_CI);
        doTestOf("utf8mb4_general_ci", UTF8MB4_GENERAL_CI);
        doTestOf("utf8mb4_bin", UTF8MB4_BIN);
        doTestOf("latin1_bin", LATIN1_BIN);
        doTestOf("latin1_general_ci", LATIN1_GENERAL_CI);
        doTestOf("latin1_general_cs", LATIN1_GENERAL_CS);
        doTestOf("cp1251_bin", CP1251_BIN);
        doTestOf("cp1251_general_ci", CP1251_GENERAL_CI);
        doTestOf("cp1251_general_cs", CP1251_GENERAL_CS);
        doTestOf("macroman_bin", MACROMAN_BIN);
        doTestOf("utf16_general_ci", UTF16_GENERAL_CI);
        doTestOf("utf16_bin", UTF16_BIN);
        doTestOf("utf16le_general_ci", UTF16LE_GENERAL_CI);
        doTestOf("cp1256_general_ci", CP1256_GENERAL_CI);
        doTestOf("cp1257_bin", CP1257_BIN);
        doTestOf("cp1257_general_ci", CP1257_GENERAL_CI);
        doTestOf("utf32_general_ci", UTF32_GENERAL_CI);
        doTestOf("utf32_bin", UTF32_BIN);
        doTestOf("utf16le_bin", UTF16LE_BIN);
        doTestOf("binary", BINARY);
        doTestOf("armscii8_bin", ARMSCII8_BIN);
        doTestOf("ascii_bin", ASCII_BIN);
        doTestOf("cp1250_bin", CP1250_BIN);
        doTestOf("cp1256_bin", CP1256_BIN);
        doTestOf("cp866_bin", CP866_BIN);
        doTestOf("dec8_bin", DEC8_BIN);
        doTestOf("greek_bin", GREEK_BIN);
        doTestOf("hebrew_bin", HEBREW_BIN);
        doTestOf("hp8_bin", HP8_BIN);
        doTestOf("keybcs2_bin", KEYBCS2_BIN);
        doTestOf("koi8r_bin", KOI8R_BIN);
        doTestOf("koi8u_bin", KOI8U_BIN);
        doTestOf("latin2_bin", LATIN2_BIN);
        doTestOf("latin5_bin", LATIN5_BIN);
        doTestOf("latin7_bin", LATIN7_BIN);
        doTestOf("cp850_bin", CP850_BIN);
        doTestOf("cp852_bin", CP852_BIN);
        doTestOf("swe7_bin", SWE7_BIN);
        doTestOf("utf8_bin", UTF8_BIN);
        doTestOf("big5_bin", BIG5_BIN);
        doTestOf("euckr_bin", EUCKR_BIN);
        doTestOf("gb2312_bin", GB2312_BIN);
        doTestOf("gbk_bin", GBK_BIN);
        doTestOf("sjis_bin", SJIS_BIN);
        doTestOf("tis620_bin", TIS620_BIN);
        doTestOf("ucs2_bin", UCS2_BIN);
        doTestOf("ujis_bin", UJIS_BIN);
        doTestOf("geostd8_general_ci", GEOSTD8_GENERAL_CI);
        doTestOf("geostd8_bin", GEOSTD8_BIN);
        doTestOf("latin1_spanish_ci", LATIN1_SPANISH_CI);
        doTestOf("cp932_japanese_ci", CP932_JAPANESE_CI);
        doTestOf("cp932_bin", CP932_BIN);
        doTestOf("eucjpms_japanese_ci", EUCJPMS_JAPANESE_CI);
        doTestOf("eucjpms_bin", EUCJPMS_BIN);
        doTestOf("cp1250_polish_ci", CP1250_POLISH_CI);
        doTestOf("utf16_unicode_ci", UTF16_UNICODE_CI);
        doTestOf("utf16_icelandic_ci", UTF16_ICELANDIC_CI);
        doTestOf("utf16_latvian_ci", UTF16_LATVIAN_CI);
        doTestOf("utf16_romanian_ci", UTF16_ROMANIAN_CI);
        doTestOf("utf16_slovenian_ci", UTF16_SLOVENIAN_CI);
        doTestOf("utf16_polish_ci", UTF16_POLISH_CI);
        doTestOf("utf16_estonian_ci", UTF16_ESTONIAN_CI);
        doTestOf("utf16_spanish_ci", UTF16_SPANISH_CI);
        doTestOf("utf16_swedish_ci", UTF16_SWEDISH_CI);
        doTestOf("utf16_turkish_ci", UTF16_TURKISH_CI);
        doTestOf("utf16_czech_ci", UTF16_CZECH_CI);
        doTestOf("utf16_danish_ci", UTF16_DANISH_CI);
        doTestOf("utf16_lithuanian_ci", UTF16_LITHUANIAN_CI);
        doTestOf("utf16_slovak_ci", UTF16_SLOVAK_CI);
        doTestOf("utf16_spanish2_ci", UTF16_SPANISH2_CI);
        doTestOf("utf16_roman_ci", UTF16_ROMAN_CI);
        doTestOf("utf16_persian_ci", UTF16_PERSIAN_CI);
        doTestOf("utf16_esperanto_ci", UTF16_ESPERANTO_CI);
        doTestOf("utf16_hungarian_ci", UTF16_HUNGARIAN_CI);
        doTestOf("utf16_sinhala_ci", UTF16_SINHALA_CI);
        doTestOf("utf16_german2_ci", UTF16_GERMAN2_CI);
        doTestOf("utf16_croatian_ci", UTF16_CROATIAN_CI);
        doTestOf("utf16_unicode_520_ci", UTF16_UNICODE_520_CI);
        doTestOf("utf16_vietnamese_ci", UTF16_VIETNAMESE_CI);
        doTestOf("ucs2_unicode_ci", UCS2_UNICODE_CI);
        doTestOf("ucs2_icelandic_ci", UCS2_ICELANDIC_CI);
        doTestOf("ucs2_latvian_ci", UCS2_LATVIAN_CI);
        doTestOf("ucs2_romanian_ci", UCS2_ROMANIAN_CI);
        doTestOf("ucs2_slovenian_ci", UCS2_SLOVENIAN_CI);
        doTestOf("ucs2_polish_ci", UCS2_POLISH_CI);
        doTestOf("ucs2_estonian_ci", UCS2_ESTONIAN_CI);
        doTestOf("ucs2_spanish_ci", UCS2_SPANISH_CI);
        doTestOf("ucs2_swedish_ci", UCS2_SWEDISH_CI);
        doTestOf("ucs2_turkish_ci", UCS2_TURKISH_CI);
        doTestOf("ucs2_czech_ci", UCS2_CZECH_CI);
        doTestOf("ucs2_danish_ci", UCS2_DANISH_CI);
        doTestOf("ucs2_lithuanian_ci", UCS2_LITHUANIAN_CI);
        doTestOf("ucs2_slovak_ci", UCS2_SLOVAK_CI);
        doTestOf("ucs2_spanish2_ci", UCS2_SPANISH2_CI);
        doTestOf("ucs2_roman_ci", UCS2_ROMAN_CI);
        doTestOf("ucs2_persian_ci", UCS2_PERSIAN_CI);
        doTestOf("ucs2_esperanto_ci", UCS2_ESPERANTO_CI);
        doTestOf("ucs2_hungarian_ci", UCS2_HUNGARIAN_CI);
        doTestOf("ucs2_sinhala_ci", UCS2_SINHALA_CI);
        doTestOf("ucs2_german2_ci", UCS2_GERMAN2_CI);
        doTestOf("ucs2_croatian_ci", UCS2_CROATIAN_CI);
        doTestOf("ucs2_unicode_520_ci", UCS2_UNICODE_520_CI);
        doTestOf("ucs2_vietnamese_ci", UCS2_VIETNAMESE_CI);
        doTestOf("ucs2_general_mysql500_ci", UCS2_GENERAL_MYSQL500_CI);
        doTestOf("utf32_unicode_ci", UTF32_UNICODE_CI);
        doTestOf("utf32_icelandic_ci", UTF32_ICELANDIC_CI);
        doTestOf("utf32_latvian_ci", UTF32_LATVIAN_CI);
        doTestOf("utf32_romanian_ci", UTF32_ROMANIAN_CI);
        doTestOf("utf32_slovenian_ci", UTF32_SLOVENIAN_CI);
        doTestOf("utf32_polish_ci", UTF32_POLISH_CI);
        doTestOf("utf32_estonian_ci", UTF32_ESTONIAN_CI);
        doTestOf("utf32_spanish_ci", UTF32_SPANISH_CI);
        doTestOf("utf32_swedish_ci", UTF32_SWEDISH_CI);
        doTestOf("utf32_turkish_ci", UTF32_TURKISH_CI);
        doTestOf("utf32_czech_ci", UTF32_CZECH_CI);
        doTestOf("utf32_danish_ci", UTF32_DANISH_CI);
        doTestOf("utf32_lithuanian_ci", UTF32_LITHUANIAN_CI);
        doTestOf("utf32_slovak_ci", UTF32_SLOVAK_CI);
        doTestOf("utf32_spanish2_ci", UTF32_SPANISH2_CI);
        doTestOf("utf32_roman_ci", UTF32_ROMAN_CI);
        doTestOf("utf32_persian_ci", UTF32_PERSIAN_CI);
        doTestOf("utf32_esperanto_ci", UTF32_ESPERANTO_CI);
        doTestOf("utf32_hungarian_ci", UTF32_HUNGARIAN_CI);
        doTestOf("utf32_sinhala_ci", UTF32_SINHALA_CI);
        doTestOf("utf32_german2_ci", UTF32_GERMAN2_CI);
        doTestOf("utf32_croatian_ci", UTF32_CROATIAN_CI);
        doTestOf("utf32_unicode_520_ci", UTF32_UNICODE_520_CI);
        doTestOf("utf32_vietnamese_ci", UTF32_VIETNAMESE_CI);
        doTestOf("utf8_unicode_ci", UTF8_UNICODE_CI);
        doTestOf("utf8_icelandic_ci", UTF8_ICELANDIC_CI);
        doTestOf("utf8_latvian_ci", UTF8_LATVIAN_CI);
        doTestOf("utf8_romanian_ci", UTF8_ROMANIAN_CI);
        doTestOf("utf8_slovenian_ci", UTF8_SLOVENIAN_CI);
        doTestOf("utf8_polish_ci", UTF8_POLISH_CI);
        doTestOf("utf8_estonian_ci", UTF8_ESTONIAN_CI);
        doTestOf("utf8_spanish_ci", UTF8_SPANISH_CI);
        doTestOf("utf8_swedish_ci", UTF8_SWEDISH_CI);
        doTestOf("utf8_turkish_ci", UTF8_TURKISH_CI);
        doTestOf("utf8_czech_ci", UTF8_CZECH_CI);
        doTestOf("utf8_danish_ci", UTF8_DANISH_CI);
        doTestOf("utf8_lithuanian_ci", UTF8_LITHUANIAN_CI);
        doTestOf("utf8_slovak_ci", UTF8_SLOVAK_CI);
        doTestOf("utf8_spanish2_ci", UTF8_SPANISH2_CI);
        doTestOf("utf8_roman_ci", UTF8_ROMAN_CI);
        doTestOf("utf8_persian_ci", UTF8_PERSIAN_CI);
        doTestOf("utf8_esperanto_ci", UTF8_ESPERANTO_CI);
        doTestOf("utf8_hungarian_ci", UTF8_HUNGARIAN_CI);
        doTestOf("utf8_sinhala_ci", UTF8_SINHALA_CI);
        doTestOf("utf8_german2_ci", UTF8_GERMAN2_CI);
        doTestOf("utf8_croatian_ci", UTF8_CROATIAN_CI);
        doTestOf("utf8_unicode_520_ci", UTF8_UNICODE_520_CI);
        doTestOf("utf8_vietnamese_ci", UTF8_VIETNAMESE_CI);
        doTestOf("utf8_general_mysql500_ci", UTF8_GENERAL_MYSQL500_CI);
        doTestOf("utf8mb4_unicode_ci", UTF8MB4_UNICODE_CI);
        doTestOf("utf8mb4_icelandic_ci", UTF8MB4_ICELANDIC_CI);
        doTestOf("utf8mb4_latvian_ci", UTF8MB4_LATVIAN_CI);
        doTestOf("utf8mb4_romanian_ci", UTF8MB4_ROMANIAN_CI);
        doTestOf("utf8mb4_slovenian_ci", UTF8MB4_SLOVENIAN_CI);
        doTestOf("utf8mb4_polish_ci", UTF8MB4_POLISH_CI);
        doTestOf("utf8mb4_estonian_ci", UTF8MB4_ESTONIAN_CI);
        doTestOf("utf8mb4_spanish_ci", UTF8MB4_SPANISH_CI);
        doTestOf("utf8mb4_swedish_ci", UTF8MB4_SWEDISH_CI);
        doTestOf("utf8mb4_turkish_ci", UTF8MB4_TURKISH_CI);
        doTestOf("utf8mb4_czech_ci", UTF8MB4_CZECH_CI);
        doTestOf("utf8mb4_danish_ci", UTF8MB4_DANISH_CI);
        doTestOf("utf8mb4_lithuanian_ci", UTF8MB4_LITHUANIAN_CI);
        doTestOf("utf8mb4_slovak_ci", UTF8MB4_SLOVAK_CI);
        doTestOf("utf8mb4_spanish2_ci", UTF8MB4_SPANISH2_CI);
        doTestOf("utf8mb4_roman_ci", UTF8MB4_ROMAN_CI);
        doTestOf("utf8mb4_persian_ci", UTF8MB4_PERSIAN_CI);
        doTestOf("utf8mb4_esperanto_ci", UTF8MB4_ESPERANTO_CI);
        doTestOf("utf8mb4_hungarian_ci", UTF8MB4_HUNGARIAN_CI);
        doTestOf("utf8mb4_sinhala_ci", UTF8MB4_SINHALA_CI);
        doTestOf("utf8mb4_german2_ci", UTF8MB4_GERMAN2_CI);
        doTestOf("utf8mb4_croatian_ci", UTF8MB4_CROATIAN_CI);
        doTestOf("utf8mb4_unicode_520_ci", UTF8MB4_UNICODE_520_CI);
        doTestOf("utf8mb4_vietnamese_ci", UTF8MB4_VIETNAMESE_CI);
        doTestOf("gb18030_chinese_ci", GB18030_CHINESE_CI);
        doTestOf("gb18030_bin", GB18030_BIN);
        doTestOf("gb18030_unicode_520_ci", GB18030_UNICODE_520_CI);
        doTestOf("utf8mb4_0900_ai_ci", UTF8MB4_0900_AI_CI);
        doTestOf("utf8mb4_de_pb_0900_ai_ci", UTF8MB4_DE_PB_0900_AI_CI);
        doTestOf("utf8mb4_is_0900_ai_ci", UTF8MB4_IS_0900_AI_CI);
        doTestOf("utf8mb4_lv_0900_ai_ci", UTF8MB4_LV_0900_AI_CI);
        doTestOf("utf8mb4_ro_0900_ai_ci", UTF8MB4_RO_0900_AI_CI);
        doTestOf("utf8mb4_sl_0900_ai_ci", UTF8MB4_SL_0900_AI_CI);
        doTestOf("utf8mb4_pl_0900_ai_ci", UTF8MB4_PL_0900_AI_CI);
        doTestOf("utf8mb4_et_0900_ai_ci", UTF8MB4_ET_0900_AI_CI);
        doTestOf("utf8mb4_es_0900_ai_ci", UTF8MB4_ES_0900_AI_CI);
        doTestOf("utf8mb4_sv_0900_ai_ci", UTF8MB4_SV_0900_AI_CI);
        doTestOf("utf8mb4_tr_0900_ai_ci", UTF8MB4_TR_0900_AI_CI);
        doTestOf("utf8mb4_cs_0900_ai_ci", UTF8MB4_CS_0900_AI_CI);
        doTestOf("utf8mb4_da_0900_ai_ci", UTF8MB4_DA_0900_AI_CI);
        doTestOf("utf8mb4_lt_0900_ai_ci", UTF8MB4_LT_0900_AI_CI);
        doTestOf("utf8mb4_sk_0900_ai_ci", UTF8MB4_SK_0900_AI_CI);
        doTestOf("utf8mb4_es_trad_0900_ai_ci", UTF8MB4_ES_TRAD_0900_AI_CI);
        doTestOf("utf8mb4_la_0900_ai_ci", UTF8MB4_LA_0900_AI_CI);
        doTestOf("utf8mb4_eo_0900_ai_ci", UTF8MB4_EO_0900_AI_CI);
        doTestOf("utf8mb4_hu_0900_ai_ci", UTF8MB4_HU_0900_AI_CI);
        doTestOf("utf8mb4_hr_0900_ai_ci", UTF8MB4_HR_0900_AI_CI);
        doTestOf("utf8mb4_vi_0900_ai_ci", UTF8MB4_VI_0900_AI_CI);
        doTestOf("utf8mb4_0900_as_cs", UTF8MB4_0900_AS_CS);
        doTestOf("utf8mb4_de_pb_0900_as_cs", UTF8MB4_DE_PB_0900_AS_CS);
        doTestOf("utf8mb4_is_0900_as_cs", UTF8MB4_IS_0900_AS_CS);
        doTestOf("utf8mb4_lv_0900_as_cs", UTF8MB4_LV_0900_AS_CS);
        doTestOf("utf8mb4_ro_0900_as_cs", UTF8MB4_RO_0900_AS_CS);
        doTestOf("utf8mb4_sl_0900_as_cs", UTF8MB4_SL_0900_AS_CS);
        doTestOf("utf8mb4_pl_0900_as_cs", UTF8MB4_PL_0900_AS_CS);
        doTestOf("utf8mb4_et_0900_as_cs", UTF8MB4_ET_0900_AS_CS);
        doTestOf("utf8mb4_es_0900_as_cs", UTF8MB4_ES_0900_AS_CS);
        doTestOf("utf8mb4_sv_0900_as_cs", UTF8MB4_SV_0900_AS_CS);
        doTestOf("utf8mb4_tr_0900_as_cs", UTF8MB4_TR_0900_AS_CS);
        doTestOf("utf8mb4_cs_0900_as_cs", UTF8MB4_CS_0900_AS_CS);
        doTestOf("utf8mb4_da_0900_as_cs", UTF8MB4_DA_0900_AS_CS);
        doTestOf("utf8mb4_lt_0900_as_cs", UTF8MB4_LT_0900_AS_CS);
        doTestOf("utf8mb4_sk_0900_as_cs", UTF8MB4_SK_0900_AS_CS);
        doTestOf("utf8mb4_es_trad_0900_as_cs", UTF8MB4_ES_TRAD_0900_AS_CS);
        doTestOf("utf8mb4_la_0900_as_cs", UTF8MB4_LA_0900_AS_CS);
        doTestOf("utf8mb4_eo_0900_as_cs", UTF8MB4_EO_0900_AS_CS);
        doTestOf("utf8mb4_hu_0900_as_cs", UTF8MB4_HU_0900_AS_CS);
        doTestOf("utf8mb4_hr_0900_as_cs", UTF8MB4_HR_0900_AS_CS);
        doTestOf("utf8mb4_vi_0900_as_cs", UTF8MB4_VI_0900_AS_CS);
        doTestOf("utf8mb4_ja_0900_as_cs", UTF8MB4_JA_0900_AS_CS);
        doTestOf("utf8mb4_ja_0900_as_cs_ks", UTF8MB4_JA_0900_AS_CS_KS);
        doTestOf("utf8mb4_0900_as_ci", UTF8MB4_0900_AS_CI);
        doTestOf("utf8mb4_ru_0900_ai_ci", UTF8MB4_RU_0900_AI_CI);
        doTestOf("utf8mb4_ru_0900_as_cs", UTF8MB4_RU_0900_AS_CS);
        doTestOf("utf8mb4_zh_0900_as_cs", UTF8MB4_ZH_0900_AS_CS);
    }

    @Test
    public void testCharsetOf() {
        Arrays.stream(CollationName.values())
            .map(Enum::name)
            .forEach(this::doTestCharsetOf);
    }

    private void doTestOf(String collation, CollationName expected) {
        Assert.assertEquals(expected, CollationName.of(collation));
        Assert.assertEquals(expected, CollationName.of(collation.toUpperCase()));
    }

    private void doTestCharsetOf(String collation) {
        CollationName collationName = CollationName.of(collation);
        CharsetName expected = Arrays.stream(CharsetName.values())
            .filter(charsetName -> charsetName.getImplementedCollationNames().contains(collationName)
                || charsetName.getUnimplementedCollationNames().contains(collationName))
            .findFirst()
            .orElse(null);

        CharsetName actual = CollationName.getCharsetOf(collation);

        Assert.assertEquals(expected, actual);
    }
}

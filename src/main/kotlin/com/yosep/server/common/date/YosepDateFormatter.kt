package com.yosep.server.common.date

import java.time.format.DateTimeFormatter

object YosepDateFormatter {
    val yyyyMmDdHhMmSsFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val yyyyMMddFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val yyyyMmDdHhMmssFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val yyyyMM: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMM")
    val yyyyMmDdHhMmSsFffFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")

    const val yyyyMMddHHmmss: String = "yyyyMMddHHmmss"
    const val yyyyMMdd: String = "yyyyMMdd"
}
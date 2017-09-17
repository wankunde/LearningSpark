case class BaseDateFeatureTable(date: String)

case class DateFeatureTable(date: String, year: Int, month: Int, day: Int, dayOfWeek: Int, dayOfYear: Int, dayNo: Long)

case class HtlBkDailySum(htl_cd: String, seg_cd: String, chn_cd: String, rm_typ: String, rt_cd: String, is_member: String, order_dt: String, live_dt: String, rns: Float, rev: Float, stat_hour: Int)

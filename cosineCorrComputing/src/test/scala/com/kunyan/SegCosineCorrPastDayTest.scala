package com.kunyan

import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
  * Created by wangcao on 2016/10/18.
  */
class SegCosineCorrPastDayTest extends FlatSpec with Matchers {

  it should "parse correctly" in {

    val utfChange = SegCosineCorrPastDay.change8To16("%u94fe%ufefe")
    utfChange should be ("%94%fe%fe%fe")

    val urlDecode = SegCosineCorrPastDay.urlcodeProcess("%u94fe%u94fe")
    urlDecode should be ("链链")

    val segWord = SegCosineCorrPastDay.segWord("我爱你塞北的雪")
    segWord should be ("我,爱,你,塞北,的,雪")

  }

}

package com.rmzx.utils

/**
  *
  */
object IsLong {
def isValidLong(t:String):Boolean = try {
  val v = t.toLong
   true
} catch {
  case e: NumberFormatException =>
    false
}
}

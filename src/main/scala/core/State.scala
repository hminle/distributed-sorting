package core

import java.util


/**
  * Created by hminle on 10/27/2016.
  */

object MasterState extends Enumeration {
  type State = Value
  val Initializing, Running, Waiting, Failed, Succeed = Value
}

object SlaveState extends Enumeration {
  type State = Value
  val Initializing, Waiting, Running, Failed, Succeed = Value
}




package riscvstorage

import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.json4s.{JString, JsonAST}
import org.json4s.JsonAST.JArray
import org.json4s.JsonDSL._

object FiltersJson {
  def jsonValue(filters: Array[Filter]): JArray = {
    JArray((filters.map {f => jsonValue(f)}).toList)
  }

  def jsonValue(filter: Filter) : JsonAST.JObject = filter match {
    case EqualTo(attribute:String, value:Any) => ("type" -> "EqualTo") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case EqualNullSafe(attribute:String, value:Any) => ("type" -> "EqualNullSafe") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case GreaterThan(attribute:String, value:Any) => ("type" -> "GreaterThan") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case GreaterThanOrEqual(attribute:String, value:Any) => ("type" -> "GreaterThanOrEqual") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case LessThan(attribute:String, value:Any) => ("type" -> "LessThan") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case LessThanOrEqual(attribute:String, value:Any) => ("type" -> "LessThanOrEqual") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case In(attribute:String, value:Array[Any]) => ("type" -> "In") ~ ("attribute" -> attribute) ~ ("value" -> JArray(value.map(v => JString(v.toString)).toList))
    case IsNotNull(attribute:String) => ("type" -> "IsNotNull") ~ ("attribute" -> attribute)
    case IsNull(attribute:String) => ("type" -> "IsNull") ~ ("attribute" -> attribute)
    case And(left:Filter, right:Filter) => ("type" -> "And") ~ ("left" -> jsonValue(left)) ~ ("right" -> jsonValue(right))
    case Or(left:Filter, right:Filter) => ("type" -> "Or") ~ ("left" -> jsonValue(left)) ~ ("right" -> jsonValue(right))
    case Not(child:Filter) => ("type" -> "Not") ~ ("child" -> jsonValue(child))
    case StringStartsWith(attribute:String, value:Any) => ("type" -> "StringStartsWith") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case StringEndsWith(attribute:String, value:Any) => ("type" -> "StringEndsWith") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case StringContains(attribute:String, value:Any) => ("type" -> "StringContains") ~ ("attribute" -> attribute) ~ ("value" -> value.toString)
    case AlwaysTrue() => ("type" -> "AlwaysTrue")
    case AlwaysFalse() => ("type" -> "AlwaysFalse")

    case _ => throw new Exception(s"Unknow Filter case class")
  }
}

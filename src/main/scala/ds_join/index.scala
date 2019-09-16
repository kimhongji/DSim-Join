package ds_join

/*Index class*/
trait Index

object IndexType {
  def apply(ty: String): IndexType = ty.toLowerCase match {
    case "jaccardindex" => JaccardIndexType
    case _ => null
  }
}

sealed abstract class IndexType

case object JaccardIndexType extends IndexType
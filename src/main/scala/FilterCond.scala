import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName) match
      case Some(value) => Some(predicate(value))
      case None => None
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = conditions.map(c => c.eval(r))
    if (results.contains(None)) None
    else {
      val booleanResults = results.map(opt => opt.get)
      Some(booleanResults.tail.foldLeft(booleanResults.head)((acc, x) => op(acc, x)))
    }
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val filtered = f.eval(r)
    filtered match
      case Some(value) => Some(!value)
      case None => None
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_&&_, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_||_, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = Compound(!_^_, List(f1, f2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val conditions = fs.map(c => c.eval(r))
    if (conditions.contains(None)) None
    else {
      val booleanResults = conditions.map(opt => opt.get)
      Some(booleanResults.contains(true))
    }
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val conditions = fs.map(c => c.eval(r))
    if (conditions.contains(None)) None
    else {
      val booleanResults = conditions.map(opt => opt.get)
      if (booleanResults.contains(false)) Some(false)
      else Some(true)
    }
  }
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}
type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {
    header.foldLeft("")((acm, x) => if(acm.isEmpty) x else acm + "," + x) + "\n"
    + tableData.foldLeft("")((acc, row) => acc +
      {if (acc.isEmpty) "" else "\n"} +
      row.values.foldLeft("")((str, y) =>
        if (str.isEmpty) y
        else str + "," + y))
  }

  def insert(row: Row): Table = {
    if (tableData.contains(row)) this
    else Table(tableName, tableData ++ List(row))
  }

  def delete(row: Row): Table = Table(tableName, tableData.filter(x => !(row == x)))

  def sort(column: String): Table = {
    val sortedData = tableData.sortWith((a, b) => a(column) < b(column))
    Table(tableName, sortedData)
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val linesToBeUpdated = filter(f).data

    val newTable = tableData.map { row =>
      if (linesToBeUpdated.contains(row)) {
        row.map { (key, value) =>
          if (updates.contains(key)) then (key, updates(key))
          else (key, value)
        }
      } else row
    }

    Table(tableName, newTable)
  }

  def filter(f: FilterCond): Table = Table(tableName, tableData.filter(row => f.eval(row) match
    case Some(value) => value
    case None => false))

  def select(columns: List[String]): Table = {
    val newData = tableData.map(row => row.filter((name, value) => columns.contains(name)))
    Table(tableName, newData)
  }

  def header: List[String] = tableData.head.keys.toList
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split("\n")
    val header = lines.head.split(",")
    val data = lines.tail.map(line => line.split(","))
    new Table(name, data.map(line => header.zip(line).toMap).toList)
  }
}

extension (table: Table) {
  def apply(i: Int): Table = Table(table.name, table.tableData(i).toString()) // Implement indexing here, find the right function to override
}

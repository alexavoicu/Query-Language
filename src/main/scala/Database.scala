case class Database(tables: List[Table]) {
  override def toString: String = tables.foldLeft("")((acc, x) => if (acc.isEmpty) x.toString else "\n" + acc + x.toString)

  def create(tableName: String): Database = {
    val isTable = tables.filter(_.name == tableName)
    if (isTable.nonEmpty) this
    else Database(tables :+ Table(tableName, ""))
  }

  def drop(tableName: String): Database = Database(tables.filter(table => !(table.name == tableName)))

  def selectTables(tableNames: List[String]): Option[Database] = {
    val tableList = tables.filter(table => tableNames.contains(table.name))
    if (tableList.size != tableNames.size) None
    else Some(Database(tableList))
  }

  def join(table1Name: String, c1: String, table2Name: String, c2: String): Option[Table] = {
    val firstTable = tables.filter(t => t.name == table1Name)
    val secondTable = tables.filter(t => t.name == table2Name)

    if (firstTable.isEmpty || secondTable.isEmpty) {
      None
    } else {
      val first = firstTable.head.data
      val second = secondTable.head.data
      val emptyKeysInB: Map[String, String] = second.head.filter((key, value) => key != c2).map((key, value) => (key, ""))
      val emptyKeysInA: Map[String, String] = first.head.map((key, value) => (key, ""))

      val rowsJustInA = first.filter(row => {
        val valueOfColumnA = row.get(c1)
        val rowInB = second.filter(r => r.get(c2) == valueOfColumnA)
        rowInB.isEmpty
      })

      val commonRows = first.filter(row => {
        val valueOfColumnA = row.get(c1)
        val rowInB = second.filter(r => r.get(c2) == valueOfColumnA)
        rowInB.nonEmpty
      })

      val rowsJustInB = second.filter(row => {
        val valueOfColumnB = row.get(c2)
        val rowInA = first.filter(r => r.get(c1) == valueOfColumnB)
        rowInA.isEmpty
      })


      val joinedTable = commonRows.map(row => {
        val valueOfColumnA = row.get(c1)
        val rowInB = second.filter(r => r.get(c2) == valueOfColumnA)
        row ++ emptyKeysInB.map {
          case (key, valueB) =>
            if (row.contains(key) && rowInB.head.contains(key) && (row.get(key) != rowInB.head.get(key))) {
              (key, row(key) + ";" + rowInB.head(key))
            } else if (row.contains(key)) (key, row(key))
            else (key, rowInB.head(key))
        }
      })

      val addedRowsJustInA = joinedTable ++ rowsJustInA.map(row => row ++
        emptyKeysInB.map((key, value) => {
          if (row.contains(key)) (key, row(key))
          else (key, value)
        })
      )

      val addedRowsJustInB = addedRowsJustInA ++ rowsJustInB.map(row => {
        emptyKeysInA.map((key, value) => if (key == c1) (key, row(c2)) else (key, value)) ++ row.removed(c2)
      })

      Some(Table("-", addedRowsJustInB))
    }
  }

  def apply(i: Int) : Table = tables(i)
}

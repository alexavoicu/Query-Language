object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT((Some(t), "FILTER", Field("name", name => name != "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] = {

    val linesToBeInserted = List(
      Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
      Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
      Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
      Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
    )

    queryT((queryT((Some(queryDB((queryDB((Some(db), "CREATE", "Inserted Fellas")),
      "SELECT", List("Inserted Fellas"))).get(0)),
      "INSERT", linesToBeInserted)),
      "SORT", "age"))
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
    val ageFilter = Field("age", age => age.nonEmpty && age.toInt < 25)
    val nameFilter = Field("name", name => name.startsWith("J"))
    val hobbyFilter = Field("hobby", hobby => hobby.nonEmpty)
    val compoundedFilter = All(List(ageFilter, nameFilter, hobbyFilter))

    queryT((queryT((Some(queryDB((Some(db), "JOIN", "People", "name", "Hobbies", "name")).get(0)),
      "FILTER", compoundedFilter)),
      "EXTRACT", List("name", "hobby")))
  }
}

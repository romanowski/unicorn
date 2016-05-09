package org.virtuslab.unicorn.dsl

import org.virtuslab.unicorn.{ BaseTest, HasJdbcDriver, TestUnicorn, Unicorn }

import scala.concurrent.ExecutionContext.Implicits.global

class EntityDslTest extends BaseTest[Long] {

  val unicorn: Unicorn[Long] with HasJdbcDriver = TestUnicorn

  abstract class TestEntityDsl extends EntityDsl(unicorn)

  import unicorn.driver.api._

  object User extends TestEntityDsl {

    case class Row(id: Option[Id],
      email: String,
      firstName: String,
      lastName: String) extends BaseRow

    class Table(tag: Tag) extends BaseTable(tag, "USERS") {
      def email = column[String]("EMAIL")

      def firstName = column[String]("FIRST_NAME")

      def lastName = column[String]("LAST_NAME")

      override def * = (id.?, email, firstName, lastName) <> (Row.tupled, Row.unapply)
    }

    override val query = TableQuery[Table]
  }

  object UsersRepository extends User.BaseRepository

  // TODO - find a way to not duplicate whole UsersRepositoryTest

  "Users Service" should "save and query users" in runWithRollback {
    for {
      repo <- UsersRepository.create()
      user = User.Row(None, "test@email.com", "Krzysztof", "Nowak")
      userId <- UsersRepository save user
      userOpt <- UsersRepository findById userId
    } yield {
      userOpt shouldBe defined

      userOpt.get should have(
        'email(user.email),
        'firstName(user.firstName),
        'lastName(user.lastName)
      )
      userOpt.get.id shouldBe defined
    }
  }

  it should "save and query multiple users" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      users = (Stream from 1 take 10) map (n => User.Row(None, "test@email.com", "Krzysztof" + n, "Nowak"))
      _ <- UsersRepository saveAll users
      newUsers <- UsersRepository.findAll()
    } yield {
      newUsers.size shouldEqual 10
      newUsers.headOption map (_.firstName) shouldEqual Some("Krzysztof1")
      newUsers.lastOption map (_.firstName) shouldEqual Some("Krzysztof10")
    }
  }

  it should "query existing user" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      user = User.Row(None, "test@email.com", "Krzysztof", "Nowak")
      userId <- UsersRepository save user
      user2 <- UsersRepository findExistingById userId
    } yield {
      user2 should have(
        'email(user.email),
        'firstName(user.firstName),
        'lastName(user.lastName)
      )
      user2.id shouldBe defined
    }
  }

  it should "update existing user" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      user = User.Row(None, "test@email.com", "Krzysztof", "Nowak")
      userId <- UsersRepository save user
      user <- UsersRepository findExistingById userId
      _ <- UsersRepository save user.copy(firstName = "Jerzy", lastName = "Muller")
      user <- UsersRepository findExistingById userId
    } yield {
      user should have(
        'email("test@email.com"),
        'firstName("Jerzy"),
        'lastName("Muller"),
        'id(Some(userId))
      )
    }
  }

  it should "query all ids" in runWithRollback {
    for {
      _ <- UsersRepository.create()
      users = Seq(
        User.Row(None, "test1@email.com", "Krzysztof", "Nowak"),
        User.Row(None, "test2@email.com", "Janek", "Nowak"),
        User.Row(None, "test3@email.com", "Marcin", "Nowak")
      )
      ids <- UsersRepository saveAll users
      all <- UsersRepository.findAll()
    } yield {
      all.flatMap(_.id) shouldEqual ids
    }
  }

  it should "sort users by id" in runWithRollback {
    for {
      _ <- UsersRepository.create()
      users = Seq(
        User.Row(None, "test1@email.com", "Krzysztof", "Nowak"),
        User.Row(None, "test2@email.com", "Janek", "Nowak"),
        User.Row(None, "test3@email.com", "Marcin", "Nowak")
      )
      ids <- UsersRepository saveAll users
      usersWithIds = (users zip ids).map { case (user, id) => user.copy(id = Some(id)) }
      usersFromDb <- UsersRepository.findAll()
    } yield {
      usersFromDb.sortBy(_.id) shouldEqual usersWithIds
    }
  }

  it should "query multiple users by ids" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      users = Seq(
        User.Row(None, "test1@email.com", "Krzysztof", "Nowak"),
        User.Row(None, "test2@email.com", "Janek", "Nowak"),
        User.Row(None, "test3@email.com", "Marcin", "Nowak")
      )
      ids <- UsersRepository saveAll users
      usersWithIds = (users zip ids).map { case (user, id) => user.copy(id = Some(id)) }
      selectedUsers = Seq(usersWithIds.head, usersWithIds.last)
      usersFromDb <- UsersRepository.findByIds(selectedUsers.flatMap(_.id))
      all <- UsersRepository.findAll()
    } yield {
      all.size shouldEqual 3
      usersFromDb shouldEqual selectedUsers
    }
  }

  it should "copy user by id" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      user = User.Row(None, "test1@email.com", "Krzysztof", "Nowak")
      id <- UsersRepository save user
      idOfCopy <- UsersRepository.copyAndSave(id)
      copiedUser <- UsersRepository.findExistingById(idOfCopy)
    } yield {
      copiedUser.id shouldNot be(user.id)

      copiedUser should have(
        'email(user.email),
        'firstName(user.firstName),
        'lastName(user.lastName)
      )
    }
  }

  it should "delete user by id" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      users = Seq(
        User.Row(None, "test1@email.com", "Krzysztof", "Nowak"),
        User.Row(None, "test2@email.com", "Janek", "Nowak"),
        User.Row(None, "test3@email.com", "Marcin", "Nowak")
      )
      ids <- UsersRepository saveAll users
      usersWithIds = (users zip ids).map { case (user, id) => user.copy(id = Some(id)) }
      beforeDelete <- UsersRepository.findAll()
      _ = usersWithIds should have size users.size
      _ <- UsersRepository.deleteById(ids(1))
      remainingUsers = Seq(usersWithIds.head, usersWithIds.last)
      all <- UsersRepository.findAll()
    } yield {
      all shouldEqual remainingUsers
    }
  }

  it should "delete all users" in runWithRollback {
    for {
      // setup
      _ <- UsersRepository.create()
      users = Seq(
        User.Row(None, "test1@email.com", "Krzysztof", "Nowak"),
        User.Row(None, "test2@email.com", "Janek", "Nowak"),
        User.Row(None, "test3@email.com", "Marcin", "Nowak")
      )
      ids <- UsersRepository saveAll users
      all <- UsersRepository.findAll()
      test = all should have size users.size
      _ <- UsersRepository.deleteAll()
      allAfterDelete <- UsersRepository.findAll()
    } yield {
      allAfterDelete shouldBe empty
    }
  }

  it should "create and drop table" in runWithRollback {
    for {
      _ <- UsersRepository.create()
      _ <- UsersRepository.drop()
    } yield true
  }
}

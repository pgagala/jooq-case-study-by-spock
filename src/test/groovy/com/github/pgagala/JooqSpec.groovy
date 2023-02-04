package com.github.pgagala

import groovy.sql.Sql
import com.github.pgagala.test_schema.Tables
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.jooq.impl.DSL
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

class JooqSpec extends Specification {

    @AutoCleanup
    @Shared
    Sql sql

    @AutoCleanup
    @Shared
    Connection connection = DriverManager.getConnection('jdbc:h2:./test_db;AUTO_SERVER=TRUE;', 'sa', '')

    @Shared
    DSLContext dlsContext

    def setupSpec() {
        sql = new Sql(connection)
        dlsContext = DSL.using(connection)
    }

    void cleanup() {
        sql.execute('DELETE FROM test_schema.foo')
    }

    def "record should be retrieved"() {
        given:
        sql.execute("INSERT INTO test_schema.foo values (223, 'bla');")
        sql.execute("INSERT INTO test_schema.foo values (12, 'foobar');")

        when:
        Result<Record> result = dlsContext.select(Tables.FOO.SOME_NUMBER, Tables.FOO.SOME_STRING)
                .from(Tables.FOO)
                .fetch()

        then:
        result.getValue(0, Tables.FOO.SOME_NUMBER) == 223
        result.getValue(0, Tables.FOO.SOME_STRING) == 'bla'
        result.getValue(1, Tables.FOO.SOME_NUMBER) == 12
        result.getValue(1, Tables.FOO.SOME_STRING) == 'foobar'
    }

    def "record should be added"() {
        when:
        dlsContext.insertInto(Tables.FOO)
                .values(345, 'xyz')
                .values(555, 'aaa')
                .execute()

        then:
        Result<Record> result = dlsContext.select(Tables.FOO.SOME_NUMBER, Tables.FOO.SOME_STRING)
                .from(Tables.FOO)
                .fetch()
        result.getValue(0, Tables.FOO.SOME_NUMBER) == 345
        result.getValue(0, Tables.FOO.SOME_STRING) == 'xyz'
        result.getValue(1, Tables.FOO.SOME_NUMBER) == 555
        result.getValue(1, Tables.FOO.SOME_STRING) == 'aaa'
    }

    def "record should be deleted"() {
        when:
        dlsContext.insertInto(Tables.FOO)
                .values(345, 'xyz')
                .values(555, 'aaa')
                .execute()

        and:
        dlsContext.deleteFrom(Tables.FOO)
                .execute()

        then:
        dlsContext.select(Tables.FOO.SOME_NUMBER, Tables.FOO.SOME_STRING)
                .from(Tables.FOO)
                .fetch()
                .isEmpty()
    }
}

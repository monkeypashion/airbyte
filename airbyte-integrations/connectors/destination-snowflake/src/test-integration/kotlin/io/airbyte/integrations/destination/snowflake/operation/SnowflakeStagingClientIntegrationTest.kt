/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake.operation

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.destination.record_buffer.FileBuffer
import io.airbyte.cdk.integrations.destination.s3.csv.CsvSerializedBuffer
import io.airbyte.cdk.integrations.destination.s3.csv.CsvSheetGenerator
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.string.Strings
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import io.airbyte.integrations.destination.snowflake.OssCloudEnvVarConsts
import io.airbyte.integrations.destination.snowflake.SnowflakeDatabaseUtils
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.util.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SnowflakeStagingClientIntegrationTest {

    private lateinit var stagingClient: SnowflakeStagingClient
    // Not using lateinit to keep spotBugs happy
    // since these vars are referenced within the setup
    // and generated bytecode as if non-null check
    private var namespace: String = ""
    private var tablename: String = ""

    private lateinit var stageName: String
    private val config =
        Jsons.deserialize(Files.readString(Paths.get("secrets/1s1t_internal_staging_config.json")))
    private val datasource =
        SnowflakeDatabaseUtils.createDataSource(config, OssCloudEnvVarConsts.AIRBYTE_OSS)
    private val database: JdbcDatabase = SnowflakeDatabaseUtils.getDatabase(datasource)
    // Intentionally not using actual columns, since the staging client should be agnostic of these
    // and only follow the order of data.

    @BeforeEach
    fun setUp() {
        namespace = Strings.addRandomSuffix("staging_client_test", "_", 5).uppercase()
        tablename = "integration_test_raw".uppercase()
        val createSchemaQuery = """
            CREATE SCHEMA "$namespace"
        """.trimIndent()
        val createStagingTableQuery =
            """
            CREATE TABLE IF NOT EXISTS "$namespace"."$tablename" (
                "id" VARCHAR PRIMARY KEY,
                "emitted_at"  TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp(),
                "data" VARIANT
            )
        """.trimIndent()
        stageName = """"$namespace"."${Strings.addRandomSuffix("stage", "_", 5)}""""
        stagingClient = SnowflakeStagingClient(database)
        database.execute(createSchemaQuery)
        stagingClient.createStageIfNotExists(stageName)
        database.execute(createStagingTableQuery)
    }

    @AfterEach
    fun tearDown() {
        stagingClient.dropStageIfExists(stageName)
        database.execute("DROP SCHEMA IF EXISTS \"$namespace\" CASCADE")
    }

    @Test
    fun verifyUploadAndCopyToTableSuccess() {
        val csvSheetGenerator =
            object : CsvSheetGenerator {
                override fun getDataRow(formattedData: JsonNode): List<Any> {
                    throw NotImplementedError("This method should not be called in this test")
                }

                override fun getDataRow(
                    id: UUID,
                    recordMessage: AirbyteRecordMessage,
                    generationId: Long,
                    syncId: Long,
                ): List<Any> {
                    throw NotImplementedError("This method should not be called in this test")
                }

                override fun getDataRow(
                    id: UUID,
                    data: List<String>,
                    emittedAt: Long,
                    formattedAirbyteMetaString: String,
                    generationId: Long
                ): List<Any> {
                    return listOf(id, Instant.ofEpochMilli(emittedAt), data)
                }

                override fun getHeaderRow(): List<String> {
                    throw NotImplementedError("This method should not be called in this test")
                }
            }
        val writeBuffer =
            CsvSerializedBuffer(
                FileBuffer(CsvSerializedBuffer.CSV_GZ_SUFFIX),
                csvSheetGenerator,
                true,
            )
        val streamId = StreamId("unused", "unused", namespace, tablename, "unused", "unused")
        val stagingPath = "${UUID.randomUUID()}/test/"
        writeBuffer.use {
            it.accept(listOf(), "", System.currentTimeMillis(), 0)
            it.accept(listOf(), "", System.currentTimeMillis(), 0)
            it.flush()
            val fileName = stagingClient.uploadRecordsToStage(writeBuffer, stageName, stagingPath)
            stagingClient.copyIntoTableFromStage(stageName, stagingPath, listOf(fileName), streamId)
        }
        val results =
            database.queryJsons(
                "SELECT * FROM \"${streamId.rawNamespace}\".\"${streamId.rawName}\""
            )
        assertTrue(results.size == 2)
        assertNotNull(results.first().get("id"))
        assertNotNull(results.first().get("emitted_at"))
        assertNotNull(results.first().get("data"))
    }

    @Test
    fun verifyUploadAndCopyToTableFailureOnMismatchedColumns() {
        val mismatchedColumnsSheetGenerator =
            object : CsvSheetGenerator {
                override fun getDataRow(formattedData: JsonNode): List<Any> {
                    throw NotImplementedError("This method should not be called in this test")
                }

                override fun getDataRow(
                    id: UUID,
                    recordMessage: AirbyteRecordMessage,
                    generationId: Long,
                    syncId: Long,
                ): List<Any> {
                    throw NotImplementedError("This method should not be called in this test")
                }

                override fun getDataRow(
                    id: UUID,
                    data: List<String>,
                    emittedAt: Long,
                    formattedAirbyteMetaString: String,
                    generationId: Long
                ): List<Any> {
                    return listOf(
                        id,
                        Instant.ofEpochMilli(emittedAt),
                        data,
                        "unknown_data_column"
                    )
                }

                override fun getHeaderRow(): List<String> {
                    throw NotImplementedError("This method should not be called in this test")
                }
            }
        val writeBuffer =
            CsvSerializedBuffer(
                FileBuffer(CsvSerializedBuffer.CSV_GZ_SUFFIX),
                mismatchedColumnsSheetGenerator,
                true,
            )
        val streamId = StreamId("unused", "unused", namespace, tablename, "unused", "unused")
        val stagingPath = "${UUID.randomUUID()}/test/"
        writeBuffer.use {
            it.accept(listOf(), "", System.currentTimeMillis(), 0)
            it.flush()
            val fileName = stagingClient.uploadRecordsToStage(writeBuffer, stageName, stagingPath)
            assertThrows(Exception::class.java) {
                stagingClient.copyIntoTableFromStage(
                    stageName,
                    stagingPath,
                    listOf(fileName),
                    streamId
                )
            }
        }
        val results =
            database.queryJsons(
                "SELECT * FROM \"${streamId.rawNamespace}\".\"${streamId.rawName}\""
            )
        assertTrue(results.isEmpty())
    }
}

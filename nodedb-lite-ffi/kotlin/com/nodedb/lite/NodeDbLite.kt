package com.nodedb.lite

import java.io.Closeable

/**
 * NodeDB-Lite — embedded edge database for Android.
 *
 * Provides vector search, graph traversal, and document CRUD
 * entirely offline. Syncs to NodeDB Origin when connectivity is available.
 *
 * Usage:
 * ```kotlin
 * val db = NodeDbLite.open("/data/data/com.myapp/databases/nodedb.db", peerId = 1L)
 * db.vectorInsert("embeddings", "v1", floatArrayOf(1.0f, 0.0f, 0.0f))
 * val results = db.vectorSearch("embeddings", floatArrayOf(1.0f, 0.0f, 0.0f), k = 5)
 * db.close()
 * ```
 */
class NodeDbLite private constructor(private var handle: Long) : Closeable {

    companion object {
        init {
            System.loadLibrary("nodedb_lite_ffi")
        }

        /**
         * Open or create a database at the given path.
         *
         * @param path Filesystem path to the SQLite database file.
         * @param peerId Unique identifier for this device (for CRDT sync).
         * @throws NodeDbException if the database cannot be opened.
         */
        fun open(path: String, peerId: Long): NodeDbLite {
            val handle = nativeOpen(path, peerId)
            if (handle == 0L) {
                throw NodeDbException("Failed to open database at $path")
            }
            return NodeDbLite(handle)
        }

        @JvmStatic private external fun nativeOpen(path: String, peerId: Long): Long
    }

    // ── Lifecycle ───────────

    /**
     * Flush all in-memory state to disk.
     * Call before app backgrounding or shutdown.
     */
    fun flush() {
        checkOpen()
        val rc = nativeFlush(handle)
        if (rc != 0) throw NodeDbException("flush failed: error $rc")
    }

    /**
     * Close the database and release all resources.
     * Safe to call multiple times.
     */
    override fun close() {
        if (handle != 0L) {
            nativeClose(handle)
            handle = 0L
        }
    }

    // ── Vector Operations ───

    /**
     * Insert a vector with an ID into a collection.
     *
     * @param collection Collection name (created automatically if new).
     * @param id Unique identifier for this vector.
     * @param embedding The vector data (float array).
     */
    fun vectorInsert(collection: String, id: String, embedding: FloatArray) {
        checkOpen()
        val rc = nativeVectorInsert(handle, collection, id, embedding, embedding.size)
        if (rc != 0) throw NodeDbException("vectorInsert failed: error $rc")
    }

    /**
     * Search for the k nearest vectors to a query.
     *
     * @return JSON string: `[{"id":"...","distance":0.123}, ...]`
     */
    fun vectorSearch(collection: String, query: FloatArray, k: Int): String {
        checkOpen()
        return nativeVectorSearch(handle, collection, query, query.size, k)
            ?: throw NodeDbException("vectorSearch failed")
    }

    /**
     * Delete a vector by ID.
     */
    fun vectorDelete(collection: String, id: String) {
        checkOpen()
        val rc = nativeVectorDelete(handle, collection, id)
        if (rc != 0) throw NodeDbException("vectorDelete failed: error $rc")
    }

    // ── Graph Operations ────

    /**
     * Insert a directed edge.
     */
    fun graphInsertEdge(from: String, to: String, edgeType: String) {
        checkOpen()
        val rc = nativeGraphInsertEdge(handle, from, to, edgeType)
        if (rc != 0) throw NodeDbException("graphInsertEdge failed: error $rc")
    }

    /**
     * Traverse the graph from a start node.
     *
     * @return JSON string: `{"nodes":[...],"edges":[...]}`
     */
    fun graphTraverse(start: String, depth: Int): String {
        checkOpen()
        return nativeGraphTraverse(handle, start, depth)
            ?: throw NodeDbException("graphTraverse failed")
    }

    // ── Document Operations ─

    /**
     * Get a document by ID.
     *
     * @return JSON string, or null if not found.
     */
    fun documentGet(collection: String, id: String): String? {
        checkOpen()
        return nativeDocumentGet(handle, collection, id)
    }

    /**
     * Put (insert or update) a document.
     *
     * @param jsonBody JSON string with `{"id":"...","fields":{...}}` format.
     */
    fun documentPut(collection: String, jsonBody: String) {
        checkOpen()
        val rc = nativeDocumentPut(handle, collection, jsonBody)
        if (rc != 0) throw NodeDbException("documentPut failed: error $rc")
    }

    /**
     * Delete a document by ID.
     */
    fun documentDelete(collection: String, id: String) {
        checkOpen()
        val rc = nativeDocumentDelete(handle, collection, id)
        if (rc != 0) throw NodeDbException("documentDelete failed: error $rc")
    }

    // ── Internal ────────────

    private fun checkOpen() {
        if (handle == 0L) throw IllegalStateException("NodeDbLite is closed")
    }

    protected fun finalize() {
        close()
    }

    // ── JNI Native Methods ──

    private external fun nativeFlush(handle: Long): Int
    private external fun nativeClose(handle: Long)
    private external fun nativeVectorInsert(
        handle: Long, collection: String, id: String,
        embedding: FloatArray, dim: Int
    ): Int
    private external fun nativeVectorSearch(
        handle: Long, collection: String, query: FloatArray,
        dim: Int, k: Int
    ): String?
    private external fun nativeVectorDelete(handle: Long, collection: String, id: String): Int
    private external fun nativeGraphInsertEdge(
        handle: Long, from: String, to: String, edgeType: String
    ): Int
    private external fun nativeGraphTraverse(handle: Long, start: String, depth: Int): String?
    private external fun nativeDocumentGet(handle: Long, collection: String, id: String): String?
    private external fun nativeDocumentPut(handle: Long, collection: String, jsonBody: String): Int
    private external fun nativeDocumentDelete(handle: Long, collection: String, id: String): Int
}

/**
 * Exception thrown by NodeDB-Lite operations.
 */
class NodeDbException(message: String) : RuntimeException(message)

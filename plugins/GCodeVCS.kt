// G-Code Version Control System
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.security.MessageDigest
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap 
import java.util.concurrent.locks.ReentrantLock 
import kotlin.concurrent.withLock
import kotlin.math.min

// Annotate data classes for serialization
@Serializable
data class GCodeMetadata(
    val lineCount: Int,
    val estimatedTime: Double?, // in minutes
    val toolChanges: Int,
    val maxFeedRate: Double?,
    val workspaceSize: Triple<Double, Double, Double>?, // X, Y, Z ranges
    val commands: Map<String, Int> // command type -> count
)

// Custom serializer for LocalDateTime
@Serializer(forClass = LocalDateTime::class)
object LocalDateTimeSerializer : KSerializer<LocalDateTime> {
    private val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("LocalDateTime", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: LocalDateTime) {
        encoder.encodeString(value.format(formatter))
    }

    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), formatter)
    }
}

// Represents a G-code commit
@Serializable
data class GCodeCommit(
    val id: String,
    val message: String,
    @Serializable(with = LocalDateTimeSerializer::class)
    val timestamp: LocalDateTime,
    // FIX 1: Audit Tracking
    val author: String, 
    val gcodeBlobIds: Map<String, String>, 
    val metadata: Map<String, GCodeMetadata>, 
    val parentId: String? = null,
    // FIX 2: Second parent for merge commits
    val parentId2: String? = null,
    val machineType: String = "Generic CNC"
)

// Branch for different machining strategies
@Serializable
data class Branch(
    val name: String,
    var commitId: String,
    val description: String = ""
)

// G-Code Version Control System
class GCodeVCS(private val repoPath: String = ".gcode_vcs") {
    // Concurrency-safe data structures
    private val commits = ConcurrentHashMap<String, GCodeCommit>()
    private val branches = ConcurrentHashMap<String, Branch>()
    private val tags = ConcurrentHashMap<String, String>()
    
    // Exclusive lock for state modifications
    private val lock = ReentrantLock() 

    @Volatile private var currentBranch = "main"
    @Volatile private var HEAD: String? = null

    // File paths for persistence
    private val commitsFilePath = "$repoPath/commits.json"
    private val branchesFilePath = "$repoPath/branches.json"
    private val tagsFilePath = "$repoPath/tags.json"
    private val headFilePath = "$repoPath/HEAD"
    private val blobsDirPath = "$repoPath/blobs"

    private val json = Json { prettyPrint = true }

    init {
        File(repoPath).mkdirs()
        File(blobsDirPath).mkdirs() 
        
        lock.withLock {
            loadState() 

            if (branches.isEmpty()) {
                branches["main"] = Branch("main", "", "Main production branch")
                saveState()
            }
            currentBranch = File(headFilePath).takeIf { it.exists() }?.readText() ?: "main"
            HEAD = branches[currentBranch]?.commitId.takeIf { it?.isNotEmpty() == true }
        }
    }

    // --- Core VCS Operations (Locked for Write Safety) ---

    // Save the entire VCS state to disk
    private fun saveState() {
        // Must be called inside a lock
        try {
            File(commitsFilePath).writeText(json.encodeToString(commits.toMap()))
            File(branchesFilePath).writeText(json.encodeToString(branches.toMap()))
            File(tagsFilePath).writeText(json.encodeToString(tags.toMap()))
            File(headFilePath).writeText(currentBranch)
        } catch (e: Exception) {
            System.err.println("FATAL: Failed to save VCS state. Data loss possible: ${e.message}")
        }
    }

    // Commit G-code files with audit tracking (FIX 1)
    fun commit(message: String, gcodeFiles: Map<String, String>, author: String, machineType: String = "Generic CNC", parent2Id: String? = null): String = lock.withLock {
        val timestamp = LocalDateTime.now()
        val metadata = gcodeFiles.mapValues { (_, content) -> analyzeGCode(content) }
        
        // Store file content as content-addressed blobs and collect blob IDs
        val gcodeBlobIds = gcodeFiles.mapValues { (_, content) ->
            val blobId = generateContentHash(content)
            File("$blobsDirPath/$blobId").writeText(content) // Write blob to disk
            blobId
        }

        val commitId = generateCommitId(message, timestamp, gcodeFiles)

        val commit = GCodeCommit(
            id = commitId,
            message = message,
            timestamp = timestamp,
            author = author, // NEW: Author tracking
            gcodeBlobIds = gcodeBlobIds, 
            metadata = metadata,
            parentId = HEAD,
            parentId2 = parent2Id, // NEW: Second parent for merges
            machineType = machineType
        )

        commits[commitId] = commit
        HEAD = commitId
        branches[currentBranch]?.commitId = commitId
        saveState() 

        println("✓ Committed: ${commitId.take(7)} - $message")
        printCommitSummary(commit)
        return commitId
    }

    // Checkout specific commit and restore G-code files
    fun checkout(commitId: String): Map<String, String>? = lock.withLock {
        val commit = commits[commitId]
        if (commit == null) {
            println("✗ Commit $commitId not found")
            return null
        }

        HEAD = commitId
        branches[currentBranch]?.commitId = commitId 
        saveState() 

        val fileContents = getCommitFiles(commit) 

        try {
            // Write files to working directory
            fileContents.forEach { (filename, content) ->
                File("$repoPath/$filename").writeText(content)
                println("  Restored: $filename")
            }
        } catch (e: Exception) {
            System.err.println("WARNING: Could not write files to disk: ${e.message}")
        }

        println("✓ Checked out commit: ${commitId.take(7)} - ${commit.message}")
        printCommitSummary(commit)
        return fileContents
    }

    // Switch to different branch
    fun switchBranch(branchName: String): Map<String, String>? = lock.withLock {
        val branch = branches[branchName]
        if (branch == null) {
            println("✗ Branch '$branchName' not found")
            return null
        }

        currentBranch = branchName
        HEAD = branch.commitId
        saveState() 

        println("✓ Switched to branch: $branchName")
        if (branch.description.isNotEmpty()) println("  ${branch.description}")

        return if (HEAD != null && HEAD!!.isNotEmpty()) {
            commits[HEAD]?.let { getCommitFiles(it) } 
        } else {
            emptyMap()
        }
    }

    // FIX 2: Merge function
    fun merge(targetBranchName: String, author: String): String? = lock.withLock {
        if (currentBranch == targetBranchName) {
            println("✗ Cannot merge a branch with itself.")
            return null
        }

        val currentCommitId = HEAD
        val targetCommitId = branches[targetBranchName]?.commitId

        if (currentCommitId == null || targetCommitId == null || currentCommitId.isEmpty() || targetCommitId.isEmpty()) {
            println("✗ Cannot merge: Current branch or target branch '$targetBranchName' has no commits.")
            return null
        }

        val currentCommit = commits[currentCommitId]!!
        val targetCommit = commits[targetCommitId]!!

        // 1. Find the Lowest Common Ancestor (LCA)
        val baseCommitId = findLCA(currentCommitId, targetCommitId)

        // 2. Handle Edge Cases
        if (baseCommitId == targetCommitId) {
            println("✓ Already up-to-date: Current branch already contains all changes from $targetBranchName.")
            return currentCommitId // Fast-forward not needed, no changes to merge
        }
        if (baseCommitId == currentCommitId) {
            println("✓ Fast-Forward Merge: Updating $currentBranch to point to $targetBranchName.")
            HEAD = targetCommitId
            branches[currentBranch]?.commitId = targetCommitId
            saveState()
            println("✓ Success: Fast-forwarded $currentBranch to ${targetCommitId.take(7)}.")
            return targetCommitId
        }

        // 3. Three-Way Merge
        val baseFiles = commits[baseCommitId]?.let { getCommitFiles(it) } ?: emptyMap()
        val currentFiles = getCommitFiles(currentCommit)
        val targetFiles = getCommitFiles(targetCommit)

        val mergeResult = applyThreeWayMerge(baseFiles, currentFiles, targetFiles)
        
        if (mergeResult.hasConflicts) {
            // Write conflicted files to disk and stop
            try {
                mergeResult.mergedFiles.forEach { (filename, content) ->
                    File("$repoPath/$filename").writeText(content)
                }
                println("✗ Merge FAILED due to conflicts. Conflicts written to working files.")
                println("   Resolve conflicts manually, then re-commit.")
                return null
            } catch (e: Exception) {
                System.err.println("FATAL: Failed to write conflicted files: ${e.message}")
                return null
            }
        }
        
        // 4. Create Merge Commit
        val mergedCommitId = commit(
            message = "Merge branch '$targetBranchName' into $currentBranch",
            gcodeFiles = mergeResult.mergedFiles,
            author = author,
            machineType = currentCommit.machineType,
            parent2Id = targetCommitId // Set second parent
        )

        println("✓ Merge SUCCESS: Created merge commit $mergedCommitId")
        return mergedCommitId
    }

    // --- Merge Helper Functions ---

    // Finds the Lowest Common Ancestor (LCA) of two commits
    private fun findLCA(id1: String, id2: String): String? {
        val ancestors1 = mutableSetOf<String>()
        var current: GCodeCommit? = commits[id1]
        
        // Collect all ancestors of id1
        while (current != null) {
            ancestors1.add(current.id)
            current = commits[current.parentId]
        }
        
        // Traverse id2's history until an ancestor of id1 is found
        current = commits[id2]
        while (current != null) {
            if (current.id in ancestors1) {
                return current.id
            }
            current = commits[current.parentId]
        }
        return null // Should only happen if one repo was branched off a very old, unpersisted repo
    }

    // Performs three-way merging on file contents
    private fun applyThreeWayMerge(base: Map<String, String>, current: Map<String, String>, target: Map<String, String>): MergeResult {
        val mergedFiles = mutableMapOf<String, String>()
        var hasConflicts = false

        val allFiles = base.keys + current.keys + target.keys
        
        for (filename in allFiles) {
            val baseContent = base[filename]
            val currentContent = current[filename]
            val targetContent = target[filename]

            when {
                // Case 1: File only exists in one branch (Add/Delete)
                currentContent == null && targetContent == null -> continue // Deleted on both, safe to ignore
                currentContent == null && baseContent != null && targetContent != null -> {
                    // Deleted in current, modified in target (Conflict: deletion vs modification)
                    mergedFiles[filename] = 
                        "<<<<<<< HEAD\n(DELETED)\n=======\n$targetContent\n>>>>>>> target\n"
                    hasConflicts = true
                }
                targetContent == null && baseContent != null && currentContent != null -> {
                    // Deleted in target, modified in current (Conflict: deletion vs modification)
                    mergedFiles[filename] = 
                        "<<<<<<< HEAD\n$currentContent\n=======\n(DELETED)\n>>>>>>> target\n"
                    hasConflicts = true
                }
                baseContent == null && currentContent != null && targetContent != null && currentContent != targetContent -> {
                    // Added on both branches, different content (Conflict: parallel addition)
                    mergedFiles[filename] = 
                        "<<<<<<< HEAD (Current Branch)\n$currentContent\n=======\n$targetContent\n>>>>>>> target ($filename)\n"
                    hasConflicts = true
                }
                baseContent == null && currentContent != null -> mergedFiles[filename] = currentContent // Added only in current
                baseContent == null && targetContent != null -> mergedFiles[filename] = targetContent // Added only in target
                
                // Case 2: File exists in all (or some)
                currentContent == targetContent -> mergedFiles[filename] = currentContent!! // Identical changes, or no changes
                currentContent == baseContent && targetContent != baseContent -> mergedFiles[filename] = targetContent!! // Modified only in target
                targetContent == baseContent && currentContent != baseContent -> mergedFiles[filename] = currentContent!! // Modified only in current
                
                // Case 3: Textual Conflict (Modified differently in both branches)
                currentContent != baseContent && targetContent != baseContent -> {
                    val mergedText = mergeGCodeText(baseContent!!, currentContent!!, targetContent!!)
                    mergedFiles[filename] = mergedText
                    if (mergedText.contains("<<<<<<<")) {
                         hasConflicts = true
                    }
                }
            }
        }
        
        return MergeResult(mergedFiles.toMap(), hasConflicts)
    }

    // Implements line-by-line G-code merging with conflict markers
    private fun mergeGCodeText(base: String, current: String, target: String): String {
        val baseLines = base.lines()
        val currentLines = current.lines()
        val targetLines = target.lines()
        
        val merged = mutableListOf<String>()
        val maxLines = maxOf(baseLines.size, currentLines.size, targetLines.size)
        
        var i = 0
        while (i < maxLines) {
            val b = baseLines.getOrElse(i) { "" }
            val c = currentLines.getOrElse(i) { "" }
            val t = targetLines.getOrElse(i) { "" }
            
            when {
                c == t -> merged.add(c) // No conflict, or identical change
                c == b -> merged.add(t) // Only modified in target
                t == b -> merged.add(c) // Only modified in current
                else -> {
                    // Conflict detected: Line changed in both branches
                    merged.add("<<<<<<< HEAD (Current Branch)")
                    merged.add(c)
                    merged.add("=======")
                    merged.add(t)
                    merged.add(">>>>>>> target")
                }
            }
            i++
        }
        return merged.joinToString("\n")
    }

    data class MergeResult(val mergedFiles: Map<String, String>, val hasConflicts: Boolean)

    // --- Utility/Read Functions (Concurrent-Safe) ---

    private fun loadState() {
        // Must be called inside a lock
        try {
            if (File(commitsFilePath).exists()) {
                commits.putAll(json.decodeFromString<Map<String, GCodeCommit>>(File(commitsFilePath).readText()))
            }
            if (File(branchesFilePath).exists()) {
                branches.putAll(json.decodeFromString<Map<String, Branch>>(File(branchesFilePath).readText()))
            }
            if (File(tagsFilePath).exists()) {
                tags.putAll(json.decodeFromString<Map<String, String>>(File(tagsFilePath).readText()))
            }
        } catch (e: FileNotFoundException) { /* ignore */ } catch (e: Exception) {
            System.err.println("WARNING: Corrupted VCS state loaded. Starting fresh: ${e.message}")
            commits.clear()
            branches.clear()
            tags.clear()
        }
    }
    
    // Function to get all files from a commit
    private fun getCommitFiles(commit: GCodeCommit): Map<String, String> {
        return commit.gcodeBlobIds.mapValues { (_, blobId) -> retrieveGCodeContent(blobId) }
    }

    // Log history (FIX 1: Prints Author)
    fun log(maxCount: Int = 10) {
        println("\n=== G-Code Commit History (${currentBranch}) ===")
        val currentCommitId = HEAD
        var count = 0
        var iterId = currentCommitId

        while (iterId != null && count < maxCount) {
            val commit = commits[iterId] ?: break
            val timeStr = commit.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            val parents = mutableListOf<String>()
            commit.parentId?.let { parents.add(it.take(7)) }
            commit.parentId2?.let { parents.add(it.take(7)) }
            val parentStr = if (parents.isNotEmpty()) " (${parents.joinToString("...")})" else ""

            println("\ncommit ${commit.id.take(7)}$parentStr (${commit.machineType})")
            println("Author: ${commit.author}") // NEW: Audit Trail
            println("Date:   $timeStr")
            println("        ${commit.message}")

            commit.metadata.forEach { (filename, meta) ->
                print("        $filename: ${meta.lineCount} lines")
                meta.estimatedTime?.let { print(", ~%.1f min".format(it)) }
                println()
            }

            iterId = commit.parentId
            count++
        }
    }

    // ... (rest of helper functions like generateContentHash, validateGCode, etc., remain the same) ...

    private fun retrieveGCodeContent(blobId: String): String {
        return try {
            val file = File("$blobsDirPath/$blobId")
            if (!file.exists()) {
                System.err.println("ERROR: Blob $blobId not found on disk.")
                return ""
            }
            file.readText()
        } catch (e: Exception) {
            System.err.println("ERROR: Could not read blob $blobId: ${e.message}")
            ""
        }
    }
    private fun printCommitSummary(commit: GCodeCommit) {
        commit.metadata.forEach { (filename, meta) ->
            println("  📄 $filename:")
            println("     Lines: ${meta.lineCount}")
            meta.estimatedTime?.let { println("     Est. Time: %.2f min".format(it)) }
            meta.maxFeedRate?.let { println("     Max Feed: $it mm/min") }
            meta.toolChanges.let { if (it > 0) println("     Tool Changes: $it") }
            meta.workspaceSize?.let { (x, y, z) ->
                println("     Workspace: X=%.2f Y=%.2f Z=%.2f".format(x, y, z))
            }
        }
    }
    private fun analyzeGCode(gcode: String): GCodeMetadata {
        val lines = gcode.lines().filter { it.trim().isNotEmpty() && !it.trim().startsWith(";") }
        val commands = mutableMapOf<String, Int>()
        var maxFeedRate = 0.0
        var toolChanges = 0
        var minX = Double.MAX_VALUE
        var maxX = -Double.MAX_VALUE 
        var minY = Double.MAX_VALUE
        var maxY = -Double.MAX_VALUE
        var minZ = Double.MAX_VALUE
        var maxZ = -Double.MAX_VALUE
        lines.forEach { line ->
            val trimmed = line.trim().split(";")[0] 
            val cmdMatch = Regex("[GM]\\d+").find(trimmed)
            if (cmdMatch != null) { commands[cmdMatch.value] = commands.getOrDefault(cmdMatch.value, 0) + 1 }
            Regex("F([\\d.]+)").find(trimmed)?.let { maxFeedRate = maxOf(maxFeedRate, it.groupValues[1].toDoubleOrNull() ?: 0.0) }
            if (trimmed.contains(Regex("M0?6|T\\d+"))) { toolChanges++ }
            Regex("X([\\d.-]+)").find(trimmed)?.let { it.groupValues[1].toDoubleOrNull()?.let { x -> if (x < minX) minX = x; if (x > maxX) maxX = x } }
            Regex("Y([\\d.-]+)").find(trimmed)?.let { it.groupValues[1].toDoubleOrNull()?.let { y -> if (y < minY) minY = y; if (y > maxY) maxY = y } }
            Regex("Z([\\d.-]+)").find(trimmed)?.let { it.groupValues[1].toDoubleOrNull()?.let { z -> if (z < minZ) minZ = z; if (z > maxZ) maxZ = z } }
        }
        val workspaceSize = if (minX != Double.MAX_VALUE) Triple(maxX - minX, maxY - minY, maxZ - minZ) else null
        return GCodeMetadata(lines.size, estimateTime(gcode), toolChanges, if (maxFeedRate > 0) maxFeedRate else null, workspaceSize, commands)
    }
    private fun estimateTime(gcode: String): Double? { /* ... time estimation logic ... */ return 0.5 }
    private fun generateContentHash(content: String): String {
        val bytes = MessageDigest.getInstance("SHA-256").digest(content.toByteArray())
        return bytes.joinToString("") { "%02x".format(it) }
    }
    private fun generateCommitId(message: String, timestamp: LocalDateTime, files: Map<String, String>): String {
        val fileContentHash = files.entries.sortedBy { it.key }.joinToString("") { "${it.key}${it.value}" }
        val content = "$message$timestamp$fileContentHash"
        val bytes = MessageDigest.getInstance("SHA-256").digest(content.toByteArray())
        return bytes.joinToString("") { "%02x".format(it) }
    }
    fun listBranches() { /* ... listBranches logic ... */ }
    fun createBranch(branchName: String, description: String = ""): Boolean = lock.withLock { /* ... createBranch logic ... */ return true }
    fun deleteBranch(branchName: String): Boolean = lock.withLock { /* ... deleteBranch logic ... */ return true }
    fun tag(tagName: String, commitId: String = HEAD ?: ""): Boolean = lock.withLock { /* ... tag logic ... */ return true }
    fun listTags() { /* ... listTags logic ... */ }
    fun gc() = lock.withLock { /* ... gc logic ... */ }

    fun getCurrentBranch() = currentBranch
    fun getHEAD() = HEAD
}

// === DEMO: Merge and Audit Trail Test ===
fun main() {
    File(".gcode_vcs").deleteRecursively()
    println("--- Starting VCS. Fresh run with Audit Tracking and Merging. ---")
    val vcs = GCodeVCS()

    // 1. Initial Commit on 'main' by Engineer A
    val gcodeA_v1 = "G1 X10 Y10 F100\nG1 Z-5 F50"
    vcs.commit("Initial safe path.", mapOf("part.gcode" to gcodeA_v1), "Alice (Tooling)")
    val mainCommit1 = vcs.getHEAD()!!

    // 2. Branch Off for Tooling Change (Current: main)
    vcs.createBranch("tool-fixture-change", "Adjusted coordinates for new fixture")
    
    // 3. Branch Off for Speed Optimization
    vcs.createBranch("speed-optimization", "Testing faster feeds on same path")
    
    // 4. Alice (Tooling) makes a change on 'tool-fixture-change'
    vcs.switchBranch("tool-fixture-change")
    val gcodeB_tool = "G1 X12 Y12 F100 ; Shifted start\nG1 Z-5 F50"
    vcs.commit("Shifted path origin to (12, 12)", mapOf("part.gcode" to gcodeB_tool), "Alice (Tooling)")
    val toolCommit = vcs.getHEAD()!!
    vcs.log(1)

    // 5. Bob (Optimization) makes a change on 'speed-optimization'
    vcs.switchBranch("speed-optimization")
    val gcodeC_speed = "G1 X10 Y10 F200 ; Faster traverse\nG1 Z-5 F150 ; Faster plunge" // Same start as base, faster feeds
    vcs.commit("Increased F rates by 100%", mapOf("part.gcode" to gcodeC_speed), "Bob (Optimization)")
    val speedCommit = vcs.getHEAD()!!
    vcs.log(1)

    // 6. Attempt Non-Conflicting Merge (Tooling -> Main)
    vcs.switchBranch("main")
    println("\n=== Merge 1: Fast-Forward Merge (Should skip) ===")
    vcs.merge("tool-fixture-change", "Alice (Tooling)")
    vcs.log(1) // Should still show mainCommit1, as main has no changes since branch

    // 7. Merge Conflicts (Optimization -> Tooling)
    vcs.switchBranch("tool-fixture-change") // Current HEAD is toolCommit (X12 Y12, F100, F50)
    println("\n=== Merge 2: Conflict Merge (Bob's F-rates vs Alice's coordinates) ===")
    // This will try to merge Bob's changes (Faster F-rates) into Alice's branch (Shifted X/Y)
    vcs.merge("speed-optimization", "Alice (Tooling)")
    
    // Check if the file contains conflicts (Merge failed)
    val conflictedFile = File(".gcode_vcs/part.gcode").readText().lines()
    println("\n--- Conflicted File Content (`part.gcode`): ---")
    conflictedFile.forEach { println(it) }
    
    if (conflictedFile.any { it.startsWith("<<<<<<<") }) {
        println("\nMerge failed. Conflicts detected (as expected).")
    }

}


import Darwin
import Foundation

enum TestFailure: Error {
    case unexpectedSuccess
    case setupFailed
}

func expectFailure(_ read: () throws -> Data) throws {
    do {
        _ = try read()
    } catch {
        return
    }
    throw TestFailure.unexpectedSuccess
}

let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
defer { try? FileManager.default.removeItem(at: directory) }
let path = directory.appendingPathComponent("media")
let offset: UInt64 = 5 * 1024 * 1024 * 1024
let expected = Data([0x80, 0xff, 0, 1])
guard FileManager.default.createFile(atPath: path.path, contents: nil) else {
    throw TestFailure.setupFailed
}
let file = try FileHandle(forWritingTo: path)
try file.seek(toOffset: offset)
try file.write(contentsOf: expected)
try file.close()

let result = try MediaFileRange.read(path: path.path, offset: Double(offset), length: 4)
precondition(result == expected)
try expectFailure { try MediaFileRange.read(path: path.path, offset: Double(offset + 1), length: 4) }
try expectFailure { try MediaFileRange.read(path: path.path, offset: 0, length: 4 * 1024 * 1024 + 1) }
try expectFailure { try MediaFileRange.read(path: path.path, offset: .infinity, length: 1) }
try expectFailure { try MediaFileRange.read(path: directory.path, offset: 0, length: 1) }

let fifo = directory.appendingPathComponent("fifo")
guard mkfifo(fifo.path, 0o600) == 0 else { throw TestFailure.setupFailed }
try expectFailure { try MediaFileRange.read(path: fifo.path, offset: 0, length: 1) }

print("React Native Apple media source passed positional read, EOF, range limit and non-regular-file tests.")

let original = directory.appendingPathComponent("original")
try Data("abcdefgh".utf8).write(to: original)
let manifest = try MediaFileRange.describe(path: original.path, chunkBytes: 4)
guard let identity = manifest["identity"] as? String, let hashes = manifest["sha256"] as? [String] else {
    throw TestFailure.setupFailed
}
precondition(hashes.count == 2)
precondition(hashes[0] == "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
let first = try MediaFileRange.read(path: original.path, offset: 0, length: 4,
                                    expectedIdentity: identity, expectedSha256: hashes[0])
precondition(first == Data("abcd".utf8))
try expectFailure {
    try MediaFileRange.read(path: original.path, offset: 0, length: 4,
                            expectedIdentity: identity, expectedSha256: hashes[1])
}

let replacement = directory.appendingPathComponent("replacement")
try Data("abcdwxyz".utf8).write(to: replacement)
guard rename(replacement.path, original.path) == 0 else { throw TestFailure.setupFailed }
try expectFailure {
    // This range has the same content on both files. The original identity must still be required.
    try MediaFileRange.read(path: original.path, offset: 0, length: 4,
                            expectedIdentity: identity, expectedSha256: hashes[0])
}
try expectFailure {
    // The digest detects changed content independently from the file identity.
    try MediaFileRange.read(path: original.path, offset: 4, length: 4, expectedSha256: hashes[1])
}
print("React Native Apple media source rejected replaced files and unmatched chunk digests.")

func peakResidentBytes() throws -> Int {
    var usage = rusage()
    guard getrusage(RUSAGE_SELF, &usage) == 0 else { throw TestFailure.setupFailed }
    return usage.ru_maxrss
}

let large = directory.appendingPathComponent("large-media")
let chunkBytes = 4 * 1024 * 1024
let chunkCount = 65
let largeBytes = chunkBytes * chunkCount
guard FileManager.default.createFile(atPath: large.path, contents: nil) else {
    throw TestFailure.setupFailed
}
let largeFile = try FileHandle(forWritingTo: large)
try largeFile.truncate(atOffset: UInt64(largeBytes))
for index in 0..<chunkCount {
    try largeFile.seek(toOffset: UInt64(index * chunkBytes))
    try largeFile.write(contentsOf: Data([UInt8(index + 1)]))
}
try largeFile.close()

let baseline = try peakResidentBytes()
let largeManifest = try MediaFileRange.describe(path: large.path, chunkBytes: Double(chunkBytes))
guard let largeIdentity = largeManifest["identity"] as? String,
      let largeHashes = largeManifest["sha256"] as? [String] else {
    throw TestFailure.setupFailed
}
precondition(largeHashes.count == chunkCount)
for (index, hash) in largeHashes.enumerated() {
    try autoreleasepool {
        let bytes = try MediaFileRange.read(path: large.path, offset: Double(index * chunkBytes),
                                            length: Double(chunkBytes), expectedIdentity: largeIdentity,
                                            expectedSha256: hash)
        let bridged = Data(base64Encoded: bytes.base64EncodedString())
        precondition(bridged?.count == chunkBytes)
        precondition(bridged?.first == UInt8(index + 1))
        precondition(bridged?.last == 0)
    }
}
let growth = try peakResidentBytes() - baseline
let budget = 96 * 1024 * 1024
precondition(growth < budget, "Media file reads increased peak resident memory by \(growth) bytes")
print("React Native Apple media source read \(largeBytes) bytes with \(growth) bytes of peak resident memory growth.")

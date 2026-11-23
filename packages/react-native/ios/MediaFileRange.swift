import CryptoKit
import Darwin
import Foundation

enum MediaFileRange {
    enum SourceChanged: Error { case changed }
    private static let maxBytes = 4 * 1024 * 1024
    private static let maxSafeInteger = 9_007_199_254_740_991
    private static let maxChunks = 65_536

    static func describe(path: String, chunkBytes: Double) throws -> [String: Any] {
        let count = try checkedLength(chunkBytes)
        return try withFile(path) { fd, info in
            let length = Int(info.st_size)
            guard (1...maxSafeInteger).contains(length), (length - 1) / count < maxChunks else {
                throw CocoaError(.fileReadTooLarge)
            }
            let identity = identityOf(info)
            var hashes: [String] = []
            var whole = SHA256()
            for offset in stride(from: 0, to: length, by: count) {
                let data = try read(fd, start: offset, count: min(count, length - offset))
                hashes.append(sha256(data))
                whole.update(data: data)
            }
            guard try identityOf(status(fd)) == identity else { throw CocoaError(.fileReadUnknown) }
            return ["version": 1, "length": length, "identity": identity, "chunkBytes": count, "sha256": hashes,
                    "fileSha256": whole.finalize().map { String(format: "%02x", $0) }.joined()]
        }
    }

    static func read(
        path: String, offset: Double, length: Double,
        expectedIdentity: String? = nil, expectedSha256: String? = nil
    ) throws -> Data {
        let count = try checkedLength(length)
        guard let start = Int(exactly: offset), start >= 0, start <= maxSafeInteger - count else {
            throw CocoaError(.fileReadInvalidFileName)
        }
        return try withFile(path) { fd, info in
            if let expectedIdentity, expectedIdentity != identityOf(info) {
                throw SourceChanged.changed
            }
            guard info.st_size >= start + count else { throw CocoaError(.fileReadUnknown) }
            let data = try read(fd, start: start, count: count)
            if let expectedIdentity {
                guard try identityOf(status(fd)) == expectedIdentity else { throw SourceChanged.changed }
            }
            if let expectedSha256, sha256(data) != expectedSha256 {
                throw SourceChanged.changed
            }
            return data
        }
    }

    private static func checkedLength(_ length: Double) throws -> Int {
        guard let count = Int(exactly: length), (1...maxBytes).contains(count) else {
            throw CocoaError(.fileReadTooLarge)
        }
        return count
    }

    private static func withFile<T>(_ path: String, _ body: (Int32, stat) throws -> T) throws -> T {
        guard path.hasPrefix("/"), !path.utf8.contains(0) else {
            throw CocoaError(.fileReadInvalidFileName)
        }
        // Nonblocking open lets us reject a FIFO without waiting for a writer.
        let fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC)
        guard fd >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer { _ = Darwin.close(fd) }
        let info = try status(fd)
        guard info.st_mode & mode_t(S_IFMT) == mode_t(S_IFREG) else {
            throw CocoaError(.fileReadUnknown)
        }
        return try body(fd, info)
    }

    private static func status(_ fd: Int32) throws -> stat {
        var info = stat()
        guard fstat(fd, &info) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return info
    }

    private static func identityOf(_ info: stat) -> String {
        "\(info.st_dev):\(info.st_ino):\(info.st_size):" +
        "\(info.st_mtimespec.tv_sec):\(info.st_mtimespec.tv_nsec):" +
        "\(info.st_ctimespec.tv_sec):\(info.st_ctimespec.tv_nsec)"
    }

    private static func sha256(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private static func read(_ fd: Int32, start: Int, count: Int) throws -> Data {
        var bytes = Data(count: count)
        try bytes.withUnsafeMutableBytes { buffer in
            guard let base = buffer.baseAddress else { throw CocoaError(.fileReadUnknown) }
            var filled = 0
            while filled < count {
                let read = pread(fd, base.advanced(by: filled), count - filled, off_t(start + filled))
                if read < 0 && errno == EINTR { continue }
                if read < 0 { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
                if read == 0 { throw CocoaError(.fileReadUnknown) }
                filled += read
            }
        }
        return bytes
    }
}

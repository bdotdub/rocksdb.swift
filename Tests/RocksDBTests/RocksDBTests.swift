import XCTest
@testable import RocksDB

struct TestRocksDataType: RocksDBValueConvertible {

    let value: String

    init(_ value: String) {
        self.value = value
    }

    init(data: Data) throws {
        try value = String(data: data) + "-DESERIALIZE"
    }

    func makeData() throws -> Data {
        return try (value + "-SERIALIZE").makeData()
    }

}

final class RocksDBTests: XCTestCase {

    var rocksDB: RocksDB!

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testInitializableGet() {
        let path = "/tmp/\(UUID().uuidString)"
        rocksDB = try! RocksDB(path: URL(fileURLWithPath: path))

        try! rocksDB.put(key: "testText", value: TestRocksDataType("test"))

        do {
            let value: TestRocksDataType = try rocksDB.get(key: "testText")
            XCTAssertEqual(value.value, "test-SERIALIZE-DESERIALIZE")
        } catch {
            XCTFail("Unable to get key")
        }

        rocksDB.closeDB()

        try! FileManager.default.removeItem(at: rocksDB.path)
    }

    func testSimplePut() {
        let path = "/tmp/\(UUID().uuidString)"
        rocksDB = try! RocksDB(path: URL(fileURLWithPath: path))

        try! rocksDB.put(key: "testText", value: "lolamkhaha")
        try! rocksDB.put(key: "testEmoji", value: "😂")
        try! rocksDB.put(key: "testTextEmoji", value: "emojitext 😂")
        try! rocksDB.put(key: "testMultipleEmoji", value: "😂😂😂")

        XCTAssertEqual(try! rocksDB.get(key: "testText"), "lolamkhaha")
        XCTAssertEqual(try! rocksDB.get(key: "testEmoji"), "😂")
        XCTAssertEqual(try! rocksDB.get(key: "testTextEmoji"), "emojitext 😂")
        XCTAssertEqual(try! rocksDB.get(key: "testMultipleEmoji"), "😂😂😂")

        rocksDB.closeDB()

        try! FileManager.default.removeItem(at: rocksDB.path)
    }

    func testSimpleDelete() {
        let path = "/tmp/\(UUID().uuidString)"
        rocksDB = try! RocksDB(path: URL(fileURLWithPath: path))

        try! rocksDB.put(key: "testDeleteKey", value: "this is a simple value 😘")
        try! rocksDB.delete(key: "testDeleteKey")

        XCTAssertEqual(try! rocksDB.get(key: "testDeleteKey"), "")

        rocksDB.closeDB()

        try! FileManager.default.removeItem(at: rocksDB.path)
    }

    func testPrefixedPut() {
        let prefixedPath = "/tmp/\(UUID().uuidString)"

        let prefixedDB = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "correctprefix")

        try! prefixedDB.put(key: "testText", value: "lolamkhaha")
        try! prefixedDB.put(key: "testEmoji", value: "😂")
        try! prefixedDB.put(key: "testTextEmoji", value: "emojitext 😂")
        try! prefixedDB.put(key: "testMultipleEmoji", value: "😂😂😂")

        XCTAssertEqual(try! prefixedDB.get(key: "testText"), "lolamkhaha")
        XCTAssertEqual(try! prefixedDB.get(key: "testEmoji"), "😂")
        XCTAssertEqual(try! prefixedDB.get(key: "testTextEmoji"), "emojitext 😂")
        XCTAssertEqual(try! prefixedDB.get(key: "testMultipleEmoji"), "😂😂😂")

        prefixedDB.closeDB()

        let wrongPrefixedDB = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "wrongprefix")

        XCTAssertEqual(try! wrongPrefixedDB.get(key: "testText"), "")
        XCTAssertEqual(try! wrongPrefixedDB.get(key: "testEmoji"), "")
        XCTAssertEqual(try! wrongPrefixedDB.get(key: "testTextEmoji"), "")
        XCTAssertEqual(try! wrongPrefixedDB.get(key: "testMultipleEmoji"), "")

        wrongPrefixedDB.closeDB()

        let prefixedDB2 = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "correctprefix")

        XCTAssertEqual(try! prefixedDB2.get(key: "testText"), "lolamkhaha")
        XCTAssertEqual(try! prefixedDB2.get(key: "testEmoji"), "😂")
        XCTAssertEqual(try! prefixedDB2.get(key: "testTextEmoji"), "emojitext 😂")
        XCTAssertEqual(try! prefixedDB2.get(key: "testMultipleEmoji"), "😂😂😂")

        prefixedDB2.closeDB()

        try! FileManager.default.removeItem(at: wrongPrefixedDB.path)
    }

    func testPrefixedDelete() {
        let prefixedPath = "/tmp/\(UUID().uuidString)"

        let prefixedDB = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "correctprefix")

        try! prefixedDB.put(key: "testText", value: "lolamkhaha")
        try! prefixedDB.put(key: "testEmoji", value: "😂")
        try! prefixedDB.put(key: "testTextEmoji", value: "emojitext 😂")
        try! prefixedDB.put(key: "testMultipleEmoji", value: "😂😂😂")

        prefixedDB.closeDB()

        let wrongPrefixedDB = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "wrongprefix")

        try! wrongPrefixedDB.put(key: "testText", value: "lolamkhaha")
        try! wrongPrefixedDB.put(key: "testEmoji", value: "😂")
        try! wrongPrefixedDB.put(key: "testTextEmoji", value: "emojitext 😂")
        try! wrongPrefixedDB.put(key: "testMultipleEmoji", value: "😂😂😂")

        wrongPrefixedDB.closeDB()

        let prefixedDB2 = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "correctprefix")

        try! prefixedDB2.delete(key: "testText")
        try! prefixedDB2.delete(key: "testEmoji")
        try! prefixedDB2.delete(key: "testTextEmoji")
        try! prefixedDB2.delete(key: "testMultipleEmoji")

        XCTAssertEqual(try! prefixedDB2.get(key: "testText"), "")
        XCTAssertEqual(try! prefixedDB2.get(key: "testEmoji"), "")
        XCTAssertEqual(try! prefixedDB2.get(key: "testTextEmoji"), "")
        XCTAssertEqual(try! prefixedDB2.get(key: "testMultipleEmoji"), "")

        prefixedDB2.closeDB()

        let wrongPrefixedDB2 = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "wrongprefix")

        XCTAssertEqual(try! wrongPrefixedDB2.get(key: "testText"), "lolamkhaha")
        XCTAssertEqual(try! wrongPrefixedDB2.get(key: "testEmoji"), "😂")
        XCTAssertEqual(try! wrongPrefixedDB2.get(key: "testTextEmoji"), "emojitext 😂")
        XCTAssertEqual(try! wrongPrefixedDB2.get(key: "testMultipleEmoji"), "😂😂😂")

        wrongPrefixedDB2.closeDB()

        try! FileManager.default.removeItem(at: wrongPrefixedDB.path)
    }

    func testSimpleIterator() {
        let path = "/tmp/\(UUID().uuidString)"
        rocksDB = try! RocksDB(path: URL(fileURLWithPath: path))

        let orderedKeysAndValues = [
            (key: "testEmoji", value: "😂"),
            (key: "testMultipleEmoji", value: "😂😂😂"),
            (key: "testText", value: "lolamkhaha"),
            (key: "testTextEmoji", value: "emojitext 😂")
        ]

        for (k, v) in orderedKeysAndValues {
            try! rocksDB.put(key: k, value: v)
        }

        var i = 0
        rocksDB.sequence().forEach { (key: String, val: String) in
            XCTAssertEqual(key, orderedKeysAndValues[i].key)
            XCTAssertEqual(val, orderedKeysAndValues[i].value)
            i += 1
        }
        XCTAssertEqual(i, 4)

        i = 1
        rocksDB.sequence(gte: "testMultipleEmoji").forEach { (key: String, val: String) in
            XCTAssertEqual(key, orderedKeysAndValues[i].key)
            XCTAssertEqual(val, orderedKeysAndValues[i].value)
            i += 1
        }
        XCTAssertEqual(i, 4)

        i = 2
        rocksDB.sequence(gte: "testText").forEach { (key: String, val: String) in
            XCTAssertEqual(key, orderedKeysAndValues[i].key)
            XCTAssertEqual(val, orderedKeysAndValues[i].value)
            i += 1
        }
        XCTAssertEqual(i, 4)

        i = 3
        rocksDB.sequence(lte: "testTextEmoji").forEach { (key: String, val: String) in
            XCTAssertEqual(key, orderedKeysAndValues[i].key)
            XCTAssertEqual(val, orderedKeysAndValues[i].value)
            i -= 1
        }
        XCTAssertEqual(i, -1)

        i = 2
        rocksDB.sequence(lte: "testText").forEach { (key: String, val: String) in
            XCTAssertEqual(key, orderedKeysAndValues[i].key)
            XCTAssertEqual(val, orderedKeysAndValues[i].value)
            i -= 1
        }
        XCTAssertEqual(i, -1)

        rocksDB.closeDB()

        try! FileManager.default.removeItem(at: rocksDB.path)
    }

    func testBatchOperations() {
        let prefixedPath = "/tmp/\(UUID().uuidString)"

        let prefixedDB = try! RocksDB(path: URL(fileURLWithPath: prefixedPath), prefix: "correctprefix")

        try! prefixedDB.put(key: "testText", value: "lolamkhaha")
        try! prefixedDB.put(key: "testEmoji", value: "😂")
        try! prefixedDB.put(key: "testTextEmoji", value: "emojitext 😂")
        try! prefixedDB.put(key: "testMultipleEmoji", value: "😂😂😂")

        try! prefixedDB.batch(operations: [
            .delete(key: "testText"),
            .put(key: "someThing", value: "someValue"),
            .delete(key: "someThing"),
            .put(key: "secondKey", value: "anotherValue"),
            .put(key: "testText", value: "textTextValue")
        ])

        XCTAssertEqual(try! prefixedDB.get(key: "testEmoji"), "😂")
        XCTAssertEqual(try! prefixedDB.get(key: "someThing"), "")
        XCTAssertEqual(try! prefixedDB.get(key: "secondKey"), "anotherValue")
        XCTAssertEqual(try! prefixedDB.get(key: "testText"), "textTextValue")

        prefixedDB.closeDB()
    }

    static var allTests = [
        ("testSimplePut", testSimplePut),
        ("testSimpleDelete", testSimpleDelete),
        ("testPrefixedPut", testPrefixedPut),
        ("testPrefixedDelete", testPrefixedDelete),
        ("testBatchOperations", testBatchOperations),
    ]
}

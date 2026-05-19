//////////////////////////////////////////////////////////////////////////////////////////////////
//
//  ConcurrencySafetyTests.swift
//  Starscream
//
//  Stress tests for transport and engine lifecycle races.
//
//////////////////////////////////////////////////////////////////////////////////////////////////

import Foundation
#if canImport(Darwin)
import Darwin
#endif
import XCTest
@testable import Starscream

final class ConcurrencySafetyTests: XCTestCase {
    func testWSEngineSurvivesConcurrentWriteAndStop() {
        let iterations = Self.environmentInt("STARSCREAM_ENGINE_STRESS_ITERATIONS", defaultValue: 200)
        let queue = DispatchQueue(label: "com.vluxe.starscream.tests.engine-stress", attributes: .concurrent)

        for iteration in 0..<iterations {
            let transport = ConcurrentTestTransport()
            let httpHandler = ImmediateUpgradeHTTPHandler()
            let engine = WSEngine(transport: transport, headerValidator: MockSecurity(), httpHandler: httpHandler)
            let delegate = RecordingEngineDelegate()
            let request = URLRequest(url: URL(string: "ws://localhost/socket")!)

            engine.register(delegate: delegate)
            engine.start(request: request)
            XCTAssertTrue(delegate.waitFor(.connected, timeout: 2), "Engine did not connect on iteration \(iteration)")

            let group = DispatchGroup()
            for writeIndex in 0..<20 {
                group.enter()
                queue.async {
                    let payload = "message-\(iteration)-\(writeIndex)".data(using: .utf8)!
                    engine.write(data: payload, opcode: .textFrame, completion: nil)
                    group.leave()
                }
            }

            group.enter()
            queue.async {
                engine.stop(closeCode: CloseCode.normal.rawValue)
                group.leave()
            }

            group.enter()
            queue.async {
                engine.forceStop()
                group.leave()
            }

            XCTAssertEqual(group.wait(timeout: .now() + .seconds(5)), .success, "Concurrent engine operations timed out on iteration \(iteration)")
        }
    }

    func testWSEngineStopBeforeUpgradeDoesNotStrandLaterWriteCompletion() {
        let iterations = Self.environmentInt("STARSCREAM_ENGINE_PRE_UPGRADE_STOP_ITERATIONS", defaultValue: 200)
        let queue = DispatchQueue(label: "com.vluxe.starscream.tests.pre-upgrade-stop", attributes: .concurrent)

        for iteration in 0..<iterations {
            let transport = ConcurrentTestTransport()
            let httpHandler = DelayedUpgradeHTTPHandler()
            let engine = WSEngine(transport: transport, headerValidator: MockSecurity(), httpHandler: httpHandler)
            let delegate = RecordingEngineDelegate()
            let request = URLRequest(url: URL(string: "ws://localhost/socket")!)
            let group = DispatchGroup()

            engine.register(delegate: delegate)
            engine.start(request: request)
            XCTAssertTrue(httpHandler.waitForConvert(timeout: 2), "Engine did not start HTTP upgrade on iteration \(iteration)")

            group.enter()
            queue.async {
                engine.stop(closeCode: CloseCode.normal.rawValue)
                group.leave()
            }

            group.enter()
            queue.async {
                engine.write(data: Data([0x01, 0x02]), opcode: .binaryFrame) {
                    group.leave()
                }
            }

            XCTAssertEqual(group.wait(timeout: .now() + .seconds(5)), .success, "Pre-upgrade stop/write timed out on iteration \(iteration)")
        }
    }

    func testWSEngineCanBeReleasedFromWriteCompletionWithoutDeadlock() {
        let iterations = Self.environmentInt("STARSCREAM_ENGINE_RELEASE_FROM_WRITE_COMPLETION_ITERATIONS", defaultValue: 200)

        for iteration in 0..<iterations {
            let completion = DispatchSemaphore(value: 0)
            var engine: WSEngine? = WSEngine(transport: ConcurrentTestTransport(),
                                             headerValidator: MockSecurity(),
                                             httpHandler: ImmediateUpgradeHTTPHandler())
            let delegate = RecordingEngineDelegate()
            let request = URLRequest(url: URL(string: "ws://localhost/socket")!)

            engine?.register(delegate: delegate)
            engine?.start(request: request)
            XCTAssertTrue(delegate.waitFor(.connected, timeout: 2), "Engine did not connect on iteration \(iteration)")

            engine?.write(data: Data([0x01]), opcode: .binaryFrame) {
                engine = nil
                completion.signal()
            }

            XCTAssertEqual(completion.wait(timeout: .now() + .seconds(5)), .success, "Engine release from write completion timed out on iteration \(iteration)")
        }
    }

#if canImport(Darwin)
    func testFoundationTransportSurvivesConcurrentWriteAndDisconnect() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_STRESS_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let queue = DispatchQueue(label: "com.vluxe.starscream.tests.foundation-transport-stress", attributes: .concurrent)
        let payload = Data(repeating: 0x2a, count: 64)

        for iteration in 0..<iterations {
            autoreleasepool {
                let transport = FoundationTransport()
                let delegate = RecordingTransportDelegate()
                transport.register(delegate: delegate)
                transport.connect(url: url, timeout: 2, certificatePinning: nil)

                guard delegate.waitForConnected(timeout: 2) else {
                    XCTFail("FoundationTransport did not connect on iteration \(iteration)")
                    return
                }

                let group = DispatchGroup()
                for _ in 0..<20 {
                    group.enter()
                    queue.async {
                        transport.write(data: payload) { _ in
                            group.leave()
                        }
                    }
                }

                group.enter()
                queue.async {
                    transport.disconnect()
                    group.leave()
                }

                XCTAssertEqual(group.wait(timeout: .now() + .seconds(5)), .success, "Concurrent FoundationTransport operations timed out on iteration \(iteration)")
                transport.disconnect()
            }
        }
    }

    func testFoundationTransportSurvivesConcurrentConnectWriteAndDisconnect() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_FULL_LIFECYCLE_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let queue = DispatchQueue(label: "com.vluxe.starscream.tests.foundation-transport-full-lifecycle", attributes: .concurrent)
        let payload = Data(repeating: 0x3c, count: 32)

        for iteration in 0..<iterations {
            autoreleasepool {
                let transport = FoundationTransport()
                let delegate = RecordingTransportDelegate()
                transport.register(delegate: delegate)

                let group = DispatchGroup()
                for _ in 0..<5 {
                    group.enter()
                    queue.async {
                        transport.connect(url: url, timeout: 2, certificatePinning: nil)
                        group.leave()
                    }

                    group.enter()
                    queue.async {
                        transport.write(data: payload) { _ in
                            group.leave()
                        }
                    }

                    group.enter()
                    queue.async {
                        transport.disconnect()
                        group.leave()
                    }
                }

                XCTAssertEqual(group.wait(timeout: .now() + .seconds(5)), .success, "Concurrent connect/write/disconnect timed out on iteration \(iteration)")
                transport.disconnect()
            }
        }
    }

    func testFoundationTransportSurvivesRapidReconnectsOnSameInstance() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_RECONNECT_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!

        let transport = FoundationTransport()
        let delegate = RecordingTransportDelegate()
        transport.register(delegate: delegate)

        for iteration in 0..<iterations {
            transport.connect(url: url, timeout: 2, certificatePinning: nil)
            XCTAssertTrue(delegate.waitForConnected(timeout: 2), "FoundationTransport did not reconnect on iteration \(iteration)")
            transport.disconnect()
        }
    }

    func testFoundationTransportWriteAfterDisconnectCompletes() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_POST_DISCONNECT_WRITE_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let payload = Data(repeating: 0x7f, count: 8)

        for iteration in 0..<iterations {
            let transport = FoundationTransport()
            let delegate = RecordingTransportDelegate()
            let completion = DispatchSemaphore(value: 0)

            transport.register(delegate: delegate)
            transport.connect(url: url, timeout: 2, certificatePinning: nil)
            XCTAssertTrue(delegate.waitForConnected(timeout: 2), "FoundationTransport did not connect on iteration \(iteration)")
            transport.disconnect()
            transport.write(data: payload) { _ in
                completion.signal()
            }

            XCTAssertEqual(completion.wait(timeout: .now() + .seconds(2)), .success, "Write after disconnect did not complete on iteration \(iteration)")
        }
    }

    func testFoundationTransportCanBeReleasedFromWriteCompletionWithoutDeadlock() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_RELEASE_FROM_WRITE_COMPLETION_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let payload = Data(repeating: 0x21, count: 8)

        for iteration in 0..<iterations {
            let completion = DispatchSemaphore(value: 0)
            var transport: FoundationTransport? = FoundationTransport()
            let delegate = RecordingTransportDelegate()

            transport?.register(delegate: delegate)
            transport?.connect(url: url, timeout: 2, certificatePinning: nil)
            XCTAssertTrue(delegate.waitForConnected(timeout: 2), "FoundationTransport did not connect on iteration \(iteration)")
            transport?.write(data: payload) { _ in
                transport = nil
                completion.signal()
            }

            XCTAssertEqual(completion.wait(timeout: .now() + .seconds(5)), .success, "FoundationTransport release from write completion timed out on iteration \(iteration)")
        }
    }

    func testFoundationTransportCanBeReleasedWhileConnectCallbacksArePending() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_CONNECT_DEINIT_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!

        for _ in 0..<iterations {
            autoreleasepool {
                let transport = FoundationTransport()
                let delegate = RecordingTransportDelegate()
                transport.register(delegate: delegate)
                transport.connect(url: url, timeout: 2, certificatePinning: nil)
            }
        }
    }

    func testFoundationTransportCanBeReleasedImmediatelyAfterWrite() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_RELEASE_AFTER_WRITE_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let payload = Data(repeating: 0x64, count: 16)

        for iteration in 0..<iterations {
            autoreleasepool {
                let transport = FoundationTransport()
                let delegate = RecordingTransportDelegate()
                transport.register(delegate: delegate)
                transport.connect(url: url, timeout: 2, certificatePinning: nil)
                XCTAssertTrue(delegate.waitForConnected(timeout: 2), "FoundationTransport did not connect on iteration \(iteration)")
                transport.write(data: payload) { _ in }
            }
        }
    }

    func testFoundationTransportCanBeReleasedWithPendingStreamCallbacks() throws {
        let iterations = Self.environmentInt("STARSCREAM_FOUNDATION_TRANSPORT_DEINIT_ITERATIONS", defaultValue: 100)
        let server = try LocalTCPDrainServer()
        let url = URL(string: "ws://127.0.0.1:\(server.port)/socket")!
        let payload = Data(repeating: 0x55, count: 32)

        for iteration in 0..<iterations {
            autoreleasepool {
                let transport = FoundationTransport()
                let delegate = RecordingTransportDelegate()
                transport.register(delegate: delegate)
                transport.connect(url: url, timeout: 2, certificatePinning: nil)

                guard delegate.waitForConnected(timeout: 2) else {
                    XCTFail("FoundationTransport did not connect on iteration \(iteration)")
                    return
                }

                for _ in 0..<10 {
                    transport.write(data: payload) { _ in }
                }
            }
        }
    }
#endif

    private static func environmentInt(_ key: String, defaultValue: Int) -> Int {
        guard let rawValue = ProcessInfo.processInfo.environment[key],
              let value = Int(rawValue),
              value > 0 else {
            return defaultValue
        }
        return value
    }
}

private final class RecordingEngineDelegate: EngineDelegate {
    private let lock = NSLock()
    private var events: [WebSocketEvent] = []
    private let connectedSemaphore = DispatchSemaphore(value: 0)

    func didReceive(event: WebSocketEvent) {
        lock.lock()
        events.append(event)
        lock.unlock()

        if case .connected = event {
            connectedSemaphore.signal()
        }
    }

    func waitFor(_ expectedEvent: ExpectedEvent, timeout: TimeInterval) -> Bool {
        if contains(expectedEvent) {
            return true
        }

        return connectedSemaphore.wait(timeout: .now() + timeout) == .success
    }

    private func contains(_ expectedEvent: ExpectedEvent) -> Bool {
        lock.lock()
        defer { lock.unlock() }

        return events.contains { event in
            switch (expectedEvent, event) {
            case (.connected, .connected):
                return true
            default:
                return false
            }
        }
    }

    enum ExpectedEvent {
        case connected
    }
}

private final class RecordingTransportDelegate: TransportEventClient {
    private let connectedSemaphore = DispatchSemaphore(value: 0)

    func connectionChanged(state: ConnectionState) {
        if case .connected = state {
            connectedSemaphore.signal()
        }
    }

    func waitForConnected(timeout: TimeInterval) -> Bool {
        connectedSemaphore.wait(timeout: .now() + timeout) == .success
    }
}

private final class ConcurrentTestTransport: Transport {
    var usingTLS: Bool {
        return false
    }

    private let lock = NSLock()
    private weak var delegate: TransportEventClient?
    private var isDisconnected = false
    private let serverFramer = WSFramer(isServer: true)

    func register(delegate: TransportEventClient) {
        lock.lock()
        self.delegate = delegate
        lock.unlock()
    }

    func connect(url: URL, timeout: Double, certificatePinning: CertificatePinning?) {
        currentDelegate()?.connectionChanged(state: .connected)
    }

    func disconnect() {
        lock.lock()
        isDisconnected = true
        lock.unlock()
    }

    func write(data: Data, completion: @escaping ((Error?) -> ())) {
        let disconnected: Bool
        lock.lock()
        disconnected = isDisconnected
        lock.unlock()

        if disconnected {
            completion(nil)
            return
        }

        _ = serverFramer.createWriteFrame(opcode: .textFrame, payload: Data(), isCompressed: false)
        completion(nil)
    }

    private func currentDelegate() -> TransportEventClient? {
        lock.lock()
        defer { lock.unlock() }
        return delegate
    }
}

private final class ImmediateUpgradeHTTPHandler: HTTPHandler {
    private weak var delegate: HTTPHandlerDelegate?
    private let callbackQueue = DispatchQueue(label: "com.vluxe.starscream.tests.immediate-upgrade-http-handler")

    func register(delegate: HTTPHandlerDelegate) {
        self.delegate = delegate
    }

    func convert(request: URLRequest) -> Data {
        callbackQueue.async { [weak self] in
            self?.delegate?.didReceiveHTTP(event: .success([:]))
        }
        return Data()
    }

    func parse(data: Data) -> Int {
        return 0
    }
}

private final class DelayedUpgradeHTTPHandler: HTTPHandler {
    private let convertSemaphore = DispatchSemaphore(value: 0)
    private weak var delegate: HTTPHandlerDelegate?

    func register(delegate: HTTPHandlerDelegate) {
        self.delegate = delegate
    }

    func convert(request: URLRequest) -> Data {
        convertSemaphore.signal()
        return Data()
    }

    func parse(data: Data) -> Int {
        return 0
    }

    func waitForConvert(timeout: TimeInterval) -> Bool {
        return convertSemaphore.wait(timeout: .now() + timeout) == .success
    }

}

#if canImport(Darwin)
private final class LocalTCPDrainServer {
    let port: UInt16

    private let socketFD: Int32
    private let acceptQueue = DispatchQueue(label: "com.vluxe.starscream.tests.local-tcp-drain.accept")
    private let clientQueue = DispatchQueue(label: "com.vluxe.starscream.tests.local-tcp-drain.clients", attributes: .concurrent)
    private let lock = NSLock()
    private var clientSockets: [Int32] = []
    private var isRunning = true

    init() throws {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else {
            throw Self.currentPOSIXError()
        }

        do {
            var reuseAddress: Int32 = 1
            guard setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuseAddress, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
                throw Self.currentPOSIXError()
            }

            var address = sockaddr_in()
            address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            address.sin_family = sa_family_t(AF_INET)
            address.sin_port = in_port_t(0).bigEndian
            address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

            let bindResult = withUnsafePointer(to: &address) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            guard bindResult == 0 else {
                throw Self.currentPOSIXError()
            }

            guard listen(fd, SOMAXCONN) == 0 else {
                throw Self.currentPOSIXError()
            }

            var boundAddress = sockaddr_in()
            var boundAddressLength = socklen_t(MemoryLayout<sockaddr_in>.size)
            let nameResult = withUnsafeMutablePointer(to: &boundAddress) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    getsockname(fd, $0, &boundAddressLength)
                }
            }
            guard nameResult == 0 else {
                throw Self.currentPOSIXError()
            }

            socketFD = fd
            port = UInt16(bigEndian: boundAddress.sin_port)
        } catch {
            close(fd)
            throw error
        }

        acceptQueue.async { [weak self] in
            self?.acceptLoop()
        }
    }

    deinit {
        cancel()
    }

    private func acceptLoop() {
        while isRunning {
            let clientSocket = accept(socketFD, nil, nil)
            if clientSocket < 0 {
                if errno == EINTR {
                    continue
                }
                return
            }

            lock.lock()
            clientSockets.append(clientSocket)
            lock.unlock()

            clientQueue.async { [weak self] in
                self?.drain(clientSocket)
            }
        }
    }

    private func drain(_ clientSocket: Int32) {
        var buffer = [UInt8](repeating: 0, count: 4096)
        while isRunning {
            let readCount = buffer.withUnsafeMutableBytes {
                recv(clientSocket, $0.baseAddress, $0.count, 0)
            }
            if readCount <= 0 {
                break
            }
        }

        close(clientSocket)
        lock.lock()
        clientSockets.removeAll { $0 == clientSocket }
        lock.unlock()
    }

    private func cancel() {
        lock.lock()
        guard isRunning else {
            lock.unlock()
            return
        }
        isRunning = false
        let sockets = clientSockets
        clientSockets.removeAll()
        lock.unlock()

        shutdown(socketFD, SHUT_RDWR)
        close(socketFD)
        sockets.forEach {
            shutdown($0, SHUT_RDWR)
            close($0)
        }
    }

    private static func currentPOSIXError() -> POSIXError {
        return POSIXError(POSIXErrorCode(rawValue: errno) ?? .EINVAL)
    }
}
#endif

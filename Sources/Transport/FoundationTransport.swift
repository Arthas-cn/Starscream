//////////////////////////////////////////////////////////////////////////////////////////////////
//
//  FoundationTransport.swift
//  Starscream
//
//  Created by Dalton Cherry on 1/23/19.
//  Copyright © 2019 Vluxe. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//////////////////////////////////////////////////////////////////////////////////////////////////

import Foundation

public enum FoundationTransportError: Error {
    case invalidRequest
    case invalidOutputStream
    case timeout
}

public class FoundationTransport: NSObject, Transport, StreamDelegate {
    private weak var delegate: TransportEventClient?
    private let workQueue = DispatchQueue(label: "com.vluxe.starscream.websocket", attributes: [])
    private let workQueueKey = DispatchSpecificKey<Void>()
    private var inputStream: InputStream?
    private var outputStream: OutputStream?
    private var isOpen = false
    private var connectionID = 0
    private var onConnect: ((InputStream, OutputStream) -> Void)?
    private var isTLS = false
    private var certPinner: CertificatePinning?
    
    public var usingTLS: Bool {
        return syncOnWorkQueue {
            return self.isTLS
        }
    }
    
    public init(streamConfiguration: ((InputStream, OutputStream) -> Void)? = nil) {
        super.init()
        workQueue.setSpecific(key: workQueueKey, value: ())
        onConnect = streamConfiguration
    }
    
    deinit {
        detachStreams()
    }
    
    public func connect(url: URL, timeout: Double = 10, certificatePinning: CertificatePinning? = nil) {
        guard let parts = url.getParts() else {
            syncOnWorkQueue {
                delegate?.connectionChanged(state: .failed(FoundationTransportError.invalidRequest))
            }
            return
        }

        syncOnWorkQueue {
            performDisconnect()
            connectionID += 1
            let currentConnectionID = connectionID

            self.certPinner = certificatePinning
            self.isTLS = parts.isTLS
            var readStream: Unmanaged<CFReadStream>?
            var writeStream: Unmanaged<CFWriteStream>?
            let h = parts.host as NSString
            CFStreamCreatePairWithSocketToHost(nil, h, UInt32(parts.port), &readStream, &writeStream)

            inputStream = readStream?.takeRetainedValue()
            outputStream = writeStream?.takeRetainedValue()
            guard let inStream = inputStream,
                  let outStream = outputStream else {
                return
            }

            inStream.delegate = self
            outStream.delegate = self
    
            if isTLS {
                let key = CFStreamPropertyKey(rawValue: kCFStreamPropertySocketSecurityLevel)
                CFReadStreamSetProperty(inStream, key, kCFStreamSocketSecurityLevelNegotiatedSSL)
                CFWriteStreamSetProperty(outStream, key, kCFStreamSocketSecurityLevelNegotiatedSSL)
            }
        
            onConnect?(inStream, outStream)
        
            isOpen = false
            CFReadStreamSetDispatchQueue(inStream, workQueue)
            CFWriteStreamSetDispatchQueue(outStream, workQueue)
            inStream.open()
            outStream.open()
        
        
            workQueue.asyncAfter(deadline: .now() + timeout, execute: { [weak self] in
                guard let s = self else { return }
                if s.connectionID == currentConnectionID && !s.isOpen {
                    s.delegate?.connectionChanged(state: .failed(FoundationTransportError.timeout))
                }
            })
        }
    }
    
    public func disconnect() {
        syncOnWorkQueue {
            connectionID += 1
            performDisconnect()
        }
    }

    private func performDisconnect() {
        detachStreams()
    }

    private func detachStreams() {
        if let stream = inputStream {
            stream.delegate = nil
            CFReadStreamSetDispatchQueue(stream, nil)
            stream.close()
        }
        if let stream = outputStream {
            stream.delegate = nil
            CFWriteStreamSetDispatchQueue(stream, nil)
            stream.close()
        }
        isOpen = false
        outputStream = nil
        inputStream = nil
    }
    
    public func register(delegate: TransportEventClient) {
        syncOnWorkQueue {
            self.delegate = delegate
        }
    }
    
    public func write(data: Data, completion: @escaping ((Error?) -> ())) {
        syncOnWorkQueue {
            guard let outStream = outputStream else {
                completion(FoundationTransportError.invalidOutputStream)
                return
            }

            if data.isEmpty {
                completion(nil)
                return
            }

            guard outStream.hasSpaceAvailable else {
                completion(FoundationTransportError.invalidOutputStream)
                return
            }

            let result = data.withUnsafeBytes { bytes -> Error? in
                guard let baseAddress = bytes.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                    return nil
                }

                var total = 0
                while total < data.count {
                    if !outStream.hasSpaceAvailable {
                        return FoundationTransportError.invalidOutputStream
                    }

                    let written = outStream.write(baseAddress.advanced(by: total), maxLength: data.count - total)
                    if written <= 0 {
                        return FoundationTransportError.invalidOutputStream
                    }
                    total += written
                }
                return nil
            }

            completion(result)
        }
    }
    
    private func getSecurityData() -> (SecTrust?, String?) {
        #if os(watchOS)
        return (nil, nil)
        #else
        guard let outputStream = outputStream else {
            return (nil, nil)
        }
        let trust = outputStream.property(forKey: kCFStreamPropertySSLPeerTrust as Stream.PropertyKey) as! SecTrust?
        var domain = outputStream.property(forKey: kCFStreamSSLPeerName as Stream.PropertyKey) as! String?
        
        if domain == nil,
            let sslContextOut = CFWriteStreamCopyProperty(outputStream, CFStreamPropertyKey(rawValue: kCFStreamPropertySSLContext)) as! SSLContext? {
            var peerNameLen: Int = 0
            SSLGetPeerDomainNameLength(sslContextOut, &peerNameLen)
            var peerName = Data(count: peerNameLen)
            let _ = peerName.withUnsafeMutableBytes { (peerNamePtr: UnsafeMutablePointer<Int8>) in
                SSLGetPeerDomainName(sslContextOut, peerNamePtr, &peerNameLen)
            }
            if let peerDomain = String(bytes: peerName, encoding: .utf8), peerDomain.count > 0 {
                domain = peerDomain
            }
        }
        return (trust, domain)
        #endif
    }
    
    private func read() {
        guard let stream = inputStream else {
            return
        }
        let maxBuffer = 4096
        let buf = NSMutableData(capacity: maxBuffer)
        let buffer = UnsafeMutableRawPointer(mutating: buf!.bytes).assumingMemoryBound(to: UInt8.self)
        let length = stream.read(buffer, maxLength: maxBuffer)
        if length < 1 {
            return
        }
        let data = Data(bytes: buffer, count: length)
        delegate?.connectionChanged(state: .receive(data))
    }

    private func syncOnWorkQueue<T>(_ work: () -> T) -> T {
        if DispatchQueue.getSpecific(key: workQueueKey) != nil {
            return work()
        }

        return workQueue.sync(execute: work)
    }
    
    // MARK: - StreamDelegate
    
    open func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
        syncOnWorkQueue {
            handleStream(aStream, eventCode: eventCode)
        }
    }

    private func handleStream(_ aStream: Stream, eventCode: Stream.Event) {
        switch eventCode {
        case .hasBytesAvailable:
            if aStream == inputStream {
                read()
            }
        case .errorOccurred:
            delegate?.connectionChanged(state: .failed(aStream.streamError))
        case .endEncountered:
            if aStream == inputStream {
                delegate?.connectionChanged(state: .cancelled)
            }
        case .openCompleted:
            if aStream == inputStream {
                let (trust, domain) = getSecurityData()
                if let pinner = certPinner, let trust = trust {
                    let currentConnectionID = connectionID
                    pinner.evaluateTrust(trust: trust, domain:  domain, completion: { [weak self] (state) in
                        self?.workQueue.async { [weak self] in
                            guard let s = self, s.connectionID == currentConnectionID, aStream == s.inputStream else {
                                return
                            }

                            switch state {
                            case .success:
                                s.isOpen = true
                                s.delegate?.connectionChanged(state: .connected)
                            case .failed(let error):
                                s.delegate?.connectionChanged(state: .failed(error))
                            }
                        }
                    })
                } else {
                    isOpen = true
                    delegate?.connectionChanged(state: .connected)
                }
            }
        default:
            break
        }
    }
}

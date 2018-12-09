'use strict';

const EventEmitter = require('events');
const Net = require('net');
const Fs = require('fs');
const Tmp = require('tmp');

const DATA_SYMBOL = Symbol('Socket-Netmsg-data');

/**
 * @enum {Number} SocketDataMode
 */
const SocketDataMode = {
    PENDING: 0,
    MESSAGE: 1,
    BINARY_LENGTH: 2,
    BINARY: 3,
};

/**
 * @enum {String} BinaryType
 */
const BinaryType = {
    BUFFER: 'b',
    FILE: 'f',
};

const copyMessageExtractingBuffers = function (message, parentKey, buffers) {

    let out;

    if (Array.isArray(message)) {
        out = [];
    }
    else if (typeof message === 'object') {
        out = {};
    }
    else {
        return message;
    }

    for (let key in message) {
        if (!message.hasOwnProperty(key)) continue;

        let val = message[key];

        if (val instanceof Buffer) {
            buffers.push({
                key: (parentKey != null ? parentKey + '.' : '') + key.replace(/\\/g, '\\\\').replace(/\./g, '\\.')
                , buffer: val,
            });
        } else if (val && (Array.isArray(val) || typeof val === 'object')) {
            out[key] = copyMessageExtractingBuffers(
                val,
                (parentKey != null ? parentKey + '.' : '') + key.replace(/\\/g, '\\\\').replace(/\./g, '\\.'),
                buffers);
        } else {
            out[key] = val;
        }
    }

    return out;
};

const splitEscapedKeys = function (key) {

    let subkeys = [];
    let i = 0, length = key.length;
    let cur = '', c;
    let escaped = false;

    for (; i < length; i++) {
        c = key[i];
        if (c === '\\') {
            escaped = !escaped;
        } else if (c === '.' && !escaped) {
            subkeys.push(cur);
            cur = '';
        } else {
            cur += c;
        }
    }

    subkeys.push(cur);

    return subkeys;
};

class Netmsg extends EventEmitter {
    constructor() {
        super();

        this._sockets = [];
    }

    /**
     * Process incoming buffers
     * @private
     * @param {Socket} socket
     * @param {Buffer?} data
     * @returns {Netmsg}
     */
    _processData(socket, data) {

        const msgdata = socket[DATA_SYMBOL];
        let message;

        switch (msgdata.mode) {

            case SocketDataMode.PENDING:

                if (data) {
                    msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
                }

                if (msgdata.buffer.length >= 4) {
                    msgdata.messageLength = msgdata.buffer.readUInt32LE(0);
                    msgdata.buffer = msgdata.buffer.slice(4);
                    msgdata.mode = SocketDataMode.MESSAGE;

                    // If there's more data available, process that
                    this._processData(socket);
                }

                break;

            case SocketDataMode.MESSAGE:

                if (data) {
                    msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
                }

                if (msgdata.buffer.length >= msgdata.messageLength) {
                    try {

                        let messageSlice = msgdata.buffer.slice(0, msgdata.messageLength);

                        msgdata.buffer = msgdata.buffer.slice(msgdata.messageLength);

                        message = JSON.parse(messageSlice.toString());

                        if (message['binaries'] && message['binaries'].length) {
                            // We have binaries to wait for
                            msgdata.pendingMessage = message;
                            message.holdingUntilGetComplete = message['binaries'].length;
                            msgdata.pendingMessage.files = {};
                            msgdata.mode = SocketDataMode.BINARY_LENGTH;
                        } else {
                            msgdata.mode = SocketDataMode.PENDING;
                        }

                        // Queue message for sending
                        this._queueIncomingMessage(socket, message);

                        // If there's more data available, process that
                        this._processData(socket);

                    } catch (ex) {
                        this.emit('error', ex);
                        socket.destroy();
                    }
                }

                break;

            case SocketDataMode.BINARY_LENGTH:

                if (data) {
                    msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
                }

                if (msgdata.buffer.length >= 4) {
                    message = msgdata.pendingMessage;

                    msgdata.binaryLength = msgdata.buffer.readUInt32LE(0);
                    msgdata.binaryDef = message['binaries'].shift();
                    msgdata.buffer = msgdata.buffer.slice(4);
                    msgdata.mode = SocketDataMode.BINARY;

                    if (msgdata.binaryDef['mode'] === BinaryType.FILE) {
                        let tmpFile = Tmp.fileSync(/**@type {Options}*/{});
                        let canFinish = true;
                        const that = this;

                        msgdata.binaryFilePath = tmpFile.name;
                        msgdata.binaryFileStream = Fs.createWriteStream(msgdata.binaryFilePath);
                        msgdata.binaryWrittenBytes = 0;

                        message.holdingUntilGetComplete++;
                        msgdata.binaryFileStream
                            .once('error', () => {
                                canFinish = false;
                            })
                            .once('finish', function () {

                                if (canFinish) {
                                    // Close Tmp file descriptor
                                    Fs.closeSync(tmpFile.fd);
                                }

                                // Close stream
                                this.close();

                                message.holdingUntilGetComplete--;
                                if (!message.holdingUntilGetComplete) {
                                    //noinspection JSAccessibilityCheck
                                    that._tryReleaseIncomingMessageQueue(socket);
                                }
                            });
                    } else {
                        msgdata.binaryBuffer = Buffer.alloc(msgdata.binaryLength);
                        msgdata.binaryWrittenBytes = 0;
                    }

                    // If there's more data available, process that
                    this._processData(socket);
                }

                break;

            case SocketDataMode.BINARY: {

                if (data) {
                    msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
                }

                let binaryDef = msgdata.binaryDef;

                if (msgdata.buffer.length) {

                    if (msgdata.binaryWrittenBytes + msgdata.buffer.length <= msgdata.binaryLength) {
                        if (binaryDef['mode'] === BinaryType.FILE) {
                            msgdata.binaryFileStream.write(msgdata.buffer);
                        }
                        else {
                            msgdata.buffer.copy(msgdata.binaryBuffer, msgdata.binaryWrittenBytes, 0,
                                msgdata.buffer.length);
                        }
                        msgdata.binaryWrittenBytes += msgdata.buffer.length;
                        msgdata.buffer = Buffer.alloc(0);
                    }
                    else {
                        let bytesToWrite = Math.min(msgdata.binaryLength - msgdata.binaryWrittenBytes,
                            msgdata.buffer.length);
                        if (binaryDef['mode'] === BinaryType.FILE) {
                            msgdata.binaryFileStream.write(msgdata.buffer.slice(0, bytesToWrite));
                        }
                        else {
                            msgdata.buffer.copy(msgdata.binaryBuffer, msgdata.binaryWrittenBytes, 0, bytesToWrite);
                        }
                        msgdata.binaryWrittenBytes += bytesToWrite;
                        msgdata.buffer = msgdata.buffer.slice(bytesToWrite);
                    }
                }

                if (msgdata.binaryWrittenBytes === msgdata.binaryLength) {
                    message = msgdata.pendingMessage;
                    message.holdingUntilGetComplete--;

                    if (binaryDef['mode'] === BinaryType.FILE) {
                        msgdata.pendingMessage.files[binaryDef['key']] = {
                            'path': msgdata.binaryFilePath,
                            'name': binaryDef['name'],
                        };
                        msgdata.binaryFileStream.end();
                    }
                    else {
                        let whereToPut = msgdata.pendingMessage.message;
                        let binaryKey = splitEscapedKeys(binaryDef['key']);
                        for (let i = 0; i < binaryKey.length - 1; i++) {
                            whereToPut = whereToPut[binaryKey[i]];
                        }
                        whereToPut[binaryKey[binaryKey.length - 1]] = msgdata.binaryBuffer;
                    }

                    if (message['binaries'].length) {
                        msgdata.mode = SocketDataMode.BINARY_LENGTH;
                    }
                    else {
                        msgdata.mode = SocketDataMode.PENDING;
                        delete msgdata.pendingMessage;
                    }

                    delete msgdata.binaryFilePath;
                    delete msgdata.binaryFileStream;
                    delete msgdata.binaryWrittenBytes;
                    delete msgdata.binaryBuffer;
                    delete msgdata.binaryDef;

                    this._tryReleaseIncomingMessageQueue(socket);

                    // If there's more data available, process that
                    this._processData(socket);
                }

                break;
            }
        }

    }

    /**
     * Queue an incoming message to be emitted
     * @private
     * @param {Socket} socket
     * @param {Object} message
     * @returns {Netmsg}
     */
    _queueIncomingMessage(socket, message) {
        let msgdata = socket[DATA_SYMBOL];

        msgdata.incomingMessages.push(message);

        return this._tryReleaseIncomingMessageQueue(socket);
    }

    /**
     * Try to dequeue a message to be emitted
     * @private
     * @param {Socket} socket
     * @returns {Netmsg}
     */
    _tryReleaseIncomingMessageQueue(socket) {
        let msgdata = socket[DATA_SYMBOL];

        while (msgdata.incomingMessages.length && !msgdata.incomingMessages[0].holdingUntilGetComplete) {
            let message = msgdata.incomingMessages.shift();

            this.emit('message', {
                message: message['message']
                , files: message['files']
                , socket: socket,
            });
        }

        return this;
    }

    /**
     * Queue a message to be sent out
     * @private
     * @param {Socket} socket
     * @param {Object} message
     * @param {Array} buffers
     * @param {Object} files
     * @returns {Netmsg}
     */
    _queueOutgoingMessage(socket, message, buffers, files) {
        let msgdata = socket[DATA_SYMBOL];

        msgdata.outgoingMessages.push({
            message: message,
            buffers: buffers,
            files: files,
            sending: false,
            holdingUntilSendComplete: buffers.length + files.length,
        });

        return this._tryReleaseOutgoingMessageQueue(socket);
    }

    /**
     * Try to dequeue a message to be sent out
     * @private
     * @param {Socket} socket
     * @returns {Netmsg}
     */
    _tryReleaseOutgoingMessageQueue(socket) {
        let msgdata = socket[DATA_SYMBOL];

        let message = msgdata.outgoingMessages[0];
        if (!message) return this;

        if (message.sending) {
            // Try to send out pending files in a message

            if (message.pendingOutgoingFile) return this;

            if (!message.holdingUntilSendComplete) {
                msgdata.outgoingMessages.shift();
                return this._tryReleaseOutgoingMessageQueue(socket);
            }

            if (message.files.length) {

                let filePath = message.files.shift();
                message.pendingOutgoingFile = filePath;
                Fs.stat(filePath, (error, stat) => {
                    if (error || !stat.isFile()) {
                        this.emit('warning', 'File with path "' + filePath + '" was not found');
                    }

                    const fileLength = (stat || {}).size || 0;

                    if (socket.writable) {
                        let lengthBuffer = Buffer.allocUnsafe(4);
                        lengthBuffer.writeUInt32LE(fileLength, 0);
                        socket.write(lengthBuffer);
                        if (fileLength <= 0) return;

                        let stream = message.pendingOutgoingStream =
                            Fs
                                .createReadStream(filePath)
                                .on('data', data => {
                                    if (!socket.writable) {
                                        return stream.close();
                                    }

                                    socket.write(data);
                                })
                                .on('error', err => {
                                    this.emit('error', err);
                                    socket.destroy();
                                    stream.close();
                                })
                                .on('end', () => {
                                    message.holdingUntilSendComplete--;
                                    delete message.pendingOutgoingStream;
                                    delete message.pendingOutgoingFile;
                                    //noinspection JSAccessibilityCheck
                                    this._tryReleaseOutgoingMessageQueue(socket);
                                    stream.close();
                                });
                    }
                });

            }

            return this;
        }

        // Make sure that the socket is writable
        if (!socket.writable) {
            msgdata.outgoingMessages.length = 0;
            return this;
        }

        message.sending = true;

        let buffer = Buffer.from(JSON.stringify(message.message));
        let lengthBuffer = Buffer.allocUnsafe(4);
        lengthBuffer.writeUInt32LE(buffer.length, 0);
        socket.write(lengthBuffer);
        socket.write(buffer);

        // Send plain buffers
        for (let i = 0; i < message.buffers.length; i++) {
            buffer = message.buffers[i];
            lengthBuffer.writeUInt32LE(buffer.length, 0);
            socket.write(lengthBuffer);
            socket.write(buffer);
            message.holdingUntilSendComplete--;
        }

        // Start sending files
        if (message.files.length) {
            return this._tryReleaseOutgoingMessageQueue(socket);
        }

        if (!message.holdingUntilSendComplete) {
            msgdata.outgoingMessages.shift();
            this._tryReleaseOutgoingMessageQueue(socket);
        }

        return this;
    }

    /**
     * Send a message to the other side
     * @param {Object} message The message to send. A message can contain `Buffer` objects.
     * @param {Object<String, {name,path}>?} files A map of files to send.
     * @returns {Netmsg}
     */
    sendMessage(message, files) {
        return this.sendMessageTo(null, message, files);
    }

    /**
     * Send a message to the other side
     * @param {Socket?} socket The socket to send to, if you want to respond to a specific peer. Null to send to all.
     * @param {Object} message The message to send. A message can contain `Buffer` objects.
     * @param {Object<String, {name,path}>?} files A map of files to send.
     * @returns {Netmsg}
     */
    sendMessageTo(socket, message, files) {
        let buffersArray = [],
            filesArray = [],
            binaries = [];

        message = copyMessageExtractingBuffers(message == null ? {} : message, null, buffersArray);

        for (let i = 0; i < buffersArray.length; i++) {
            binaries.push({
                'mode': BinaryType.BUFFER
                , 'key': buffersArray[i].key,
            });
            buffersArray[i] = buffersArray[i].buffer;
        }

        if (files) {
            for (let key in files) {
                if (!files.hasOwnProperty(key)) continue;

                let file = files[key];
                binaries.push({
                    'mode': BinaryType.FILE
                    , 'key': key
                    , 'name': file['name'],
                });
                filesArray.push(file['path']);
            }
        }

        if (socket) {
            this._queueOutgoingMessage(
                socket,
                {
                    'message': message,
                    'binaries': binaries.length ? binaries : false,
                },
                buffersArray,
                filesArray
            );
        } else {
            for (let socket of this._sockets) {
                this._queueOutgoingMessage(
                    socket,
                    {
                        'message': message,
                        'binaries': binaries ? binaries : false,
                    },
                    buffersArray,
                    filesArray
                );
            }
        }

        return this;
    }

    /**
     * Start a local server
     * @param {Object} options
     * @param {String?} options.host
     * @param {Number?} options.port
     * @param {Number?} options.backlog
     * @param {String?} options.path
     * @param {Boolean?} options.exclusive
     * @returns {Netmsg}
     */
    listen(options) {
        this.stopServer();

        let server = Net.createServer(socket => {
            this.listenOnSocket(socket);

            this.emit('connect', socket);
        });

        server
            .on('error', err => {
                // Forward errors
                this.emit('error', err);
            })
            .on('listening', () => {
                this.emit('listening');
            });

        this._listenerServer = server;

        server.listen(options);

        return this;
    }

    /**
     * Stop listening
     * @param {function?} callback
     * @return {Netmsg}
     */
    stopServer(callback) {
        if (!this._listenerServer) {
            callback && callback(new Error('The Netmsg instance was not listening'));
            return this;
        }

        this._listenerServer.close(callback);
        this._listenerServer = null;

        return this;
    }

    /**
     * Stop listening on a specific socket
     * @param {Socket} socket Socket to listen on
     * @returns {Netmsg}
     */
    stopListeningOnSocket(socket) {
        let i = this._sockets.indexOf(socket);
        if (i > -1) {
            this._sockets.splice(i, 1);

            const msgdata = socket[DATA_SYMBOL];
            if (msgdata && msgdata.events) {
                socket.removeListener('data', msgdata.events.onData);
                socket.removeListener('end', msgdata.events.onEnd);
                socket.removeListener('error', msgdata.events.onError);
                socket.removeListener('close', msgdata.events.onClose);
            }
        }

        return this;
    }

    /**
     * Start listening on a specific socket
     * @param {Socket} socket Socket to listen on
     * @returns {Netmsg}
     */
    listenOnSocket(socket) {
        this.stopListeningOnSocket(socket);

        let events = {
            onData: (data) => {
                this._processData(socket, data);
            },

            onClose: (/*hadError*/) => {
                this.stopListeningOnSocket(socket);
                this.emit('disconnect', socket);
            },

            onError: () => {
                // Ignore these for now. 'close' will be sent anyway after an error.
            },

            onEnd: ()  => {
                // Graceful shutdown from the other end
            },
        };

        socket[DATA_SYMBOL] = {
            buffer: Buffer.alloc(0)
            , mode: SocketDataMode.PENDING
            , incomingMessages: []
            , outgoingMessages: []
            , events: events,
        };

        this._sockets.push(socket);

        socket.on('data', events.onData);
        socket.on('end', events.onEnd);
        socket.on('error', events.onError);
        socket.on('close', events.onClose);

        return this;
    }

    /**
     * Connect to a remote server
     * @param {Object} options
     * @param {String=localhost} options.host For TCP sockets, host the client should connect to. Defaults to `'localhost'`.
     * @param {Number?} options.port For TCP sockets, port the client should connect to (Required)
     * @param {String?} options.path For local domain sockets, path the client should connect to. (Required)
     * @param {Number=4} options.family Version of IP stack. Defaults to 4.
     * @param {Number=0} options.hints `dns.lookup()` hints. Defaults to 0.
     * @param {Function?} options.lookup Custom lookup function. Defaults to `dns.lookup`.
     * @param {Number?} options.timeout Will cancel the `connect` if `timeout` has passed and `connect` was not fired.
     * @param {Number?} options.totalTimeout Will stop any connection/retry attempt after specified timeout.
     * @param {Number=0} options.retry How many times to retry when encountering EHOSTUNREACH or ETIMEOUT.
     * @param {Boolean=true} options.noDelay Disables the Nagle algorithm.
     * @returns {Netmsg}
     */
    connect(options) {

        let that = this;

        let socket = null;

        let retryCount = options.retry || 0;

        let timeoutMs =
            (options.timeout && isFinite(options.timeout) && options.timeout > 0)
                ? options.timeout
                : 0;
        let totalTimeoutMs =
            (options.totalTimeout && isFinite(options.totalTimeout) && options.totalTimeout > 0)
                ? options.totalTimeout
                : 0;

        let timeout = null, totalTimeout = null;

        // Abort any previous `connect` operation
        if (this._connectingSocket) {
            let error = new Error();
            error.code = 'ECANCELED';
            this._connectingSocket.destroy(error);
            delete this._connectingSocket;
        }

        function connect () {
            if (timeoutMs) {
                timeout = setTimeout(function () {
                    timeout = null;
                    if (socket.connecting) {
                        let error = new Error();
                        error.code = 'ETIMEOUT';
                        socket.destroy(error); // This emits both ETIMEOUT and ECANCELED
                    }
                }, timeoutMs);
            }

            socket = new Net.Socket({ allowHalfOpen: true })
                .on('error', onError)
                .on('connect', onConnect);

            let noDelay = options.noDelay === undefined ? true : !!options.noDelay;
            socket.setNoDelay(noDelay);

            socket.connect(options);

            that._connectingSocket = socket;
        }

        function onError (err) {
            if (err && err.code === 'ECANCELED') return;

            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            if (totalTimeout) {
                clearTimeout(totalTimeout);
                totalTimeout = null;
            }

            if (err.code === 'EHOSTUNREACH' || err.code === 'ETIMEOUT') {
                if (retryCount) {
                    retryCount--;
                    connect();
                    return;
                }

                // No retry, remove listeners here
                socket.removeAllListeners();

                // Forward errors
                that.emit('retryerror', err);

                return;
            }

            // Forward errors
            that.emit('error', err);
        }

        function onConnect (err) {

            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            if (totalTimeout) {
                clearTimeout(totalTimeout);
                totalTimeout = null;
            }

            if (err) {
                that.emit('error', err);
                return;
            }

            if (that._clientSocket) {
                that._clientSocket.destroy();
                that.stopListeningOnSocket(that._clientSocket);
            }

            delete that._connectingSocket;
            that._clientSocket = socket;
            that.listenOnSocket(socket);

            that.emit('connect', socket);
        }

        // Start connection attempt
        connect();

        // Set global timeout
        if (totalTimeoutMs) {
            totalTimeout = setTimeout(() => {
                retryCount = 0;
                if (socket.connecting) {
                    let error = new Error();
                    error.code = 'ETIMEOUT';
                    socket.destroy(error); // This emits both ETIMEOUT and ECANCELED
                }
            }, totalTimeoutMs);
        }

        return that;
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Disconnect client from a remote server
     * @returns {Netmsg}
     */
    disconnectClient() {
        if (this._clientSocket) {
            this._clientSocket.destroy();
            this.stopListeningOnSocket(this._clientSocket);
        }

        return this;
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Disconnect all sockets
     * @returns {Netmsg}
     */
    disconnectAll() {
        let sockets = this.getAllSockets();

        for (let socket of sockets) {
            socket.destroy();
            this.stopListeningOnSocket(socket);
        }

        return this;
    }

    /**
     * Retrieve and array of all sockets.
     * The array is *not* a reference to an internal array.
     * @returns {Array.<Socket>}
     */
    getAllSockets() {
        return this._sockets.slice(0);
    }
}

/** @type {Netmsg} */
module.exports = Netmsg;

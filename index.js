'use strict';

const Util = require('util');
const EventEmitter = require('events');
const Net = require('net');
const Fs = require('fs');
const Tmp = require('tmp');

var SocketDataMode = {
    PENDING: 0,
    MESSAGE: 1,
    BINARY_LENGTH: 2,
    BINARY: 3
};

var BinaryType = {
    BUFFER: 'b',
    FILE: 'f'
};

var copyMessageExtractingBuffers = function (message, parentKey, buffers) {

    var out;

    if (Array.isArray(message)) {
        out = [];
    }
    else if (typeof message === 'object') {
        out = {};
    }
    else {
        return message;
    }

    for (var key in message) {
        if (!message.hasOwnProperty(key)) continue;
        var val = message[key];

        if (val instanceof Buffer) {
            buffers.push({
                key: (parentKey != null ? parentKey + '.' : '') + key.replace(/\\/g, '\\\\').replace(/\./g, '\\.')
                , buffer: val
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

var splitEscapedKeys = function (key) {

    var subkeys = [];
    var i = 0, length = key.length;
    var cur = '', c;
    var escaped = false;

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

var Netmsg = function Netmsg (socket) {
    // Allow instansiation without `new`
    if (!(this instanceof Netmsg)) {
        return new (Function.prototype.bind.apply(
            Netmsg,
            [Netmsg].concat(Array.prototype.slice.call(arguments, 0))));
    }

    var that = this;

    EventEmitter.call(this);

    this._listenerServers = [];
    this._sockets = [];

    /** @this {Socket} */
    that._onData = function (data) {
        var socket = this;
        that._processData(socket, data);
    };

    /** @this {Socket} */
    that._onEnd = function () {
        // Graceful shutdown from the other end
    };

    /** @this {Socket} */
    that._onError = function () {
        // Ignore these for now. 'close' will be sent anyway after an error.
    };

    //noinspection JSUnusedLocalSymbols
    /** @this {Socket} */
    that._onClose = function (had_error) {
        var socket = this;
        that.stopListeningOnSocket(socket);
        that.emit('disconnect', socket);
    };
};

Util.inherits(Netmsg, EventEmitter);

/**
 * Process incoming buffers
 * @private
 * @param {Socket} socket
 * @param {Buffer?} data
 * @returns {Netmsg}
 */
Netmsg.prototype._processData = function (socket, data) {

    var that = this;

    var msgdata = socket.__msgdata;
    var message;

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
                that._processData(socket);
            }

            break;

        case SocketDataMode.MESSAGE:

            if (data) {
                msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
            }

            if (msgdata.buffer.length >= msgdata.messageLength) {
                try {

                    var messageSlice = msgdata.buffer.slice(0, msgdata.messageLength);

                    msgdata.buffer = msgdata.buffer.slice(msgdata.messageLength);

                    message = JSON.parse(messageSlice.toString());

                    if (message['binaries'] && message['binaries'].length) {
                        // We have binaries to wait for
                        msgdata.pendingMessage = message;
                        message.holdingUntilSend = message['binaries'].length;
                        msgdata.pendingMessage.files = {};
                        msgdata.mode = SocketDataMode.BINARY_LENGTH;
                    } else {
                        msgdata.mode = SocketDataMode.PENDING;
                    }

                    // Queue message for sending
                    that._queueIncomingMessage(socket, message);

                    // If there's more data available, process that
                    that._processData(socket);

                } catch (ex) {
                    that.emit('error', ex);
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
                    msgdata.binaryFilePath = Tmp.fileSync({}).name;
                    msgdata.binaryFileStream = Fs.createWriteStream(msgdata.binaryFilePath);
                    msgdata.binaryWrittenBytes = 0;

                    message.holdingUntilSend++;
                    msgdata.binaryFileStream.on('finish', function () {
                        message.holdingUntilSend--;
                        if (!message.holdingUntilSend) {
                            //noinspection JSAccessibilityCheck
                            that._tryReleaseIncomingMessageQueue(socket);
                        }
                    });
                } else {
                    msgdata.binaryBuffer = new Buffer(msgdata.binaryLength);
                    msgdata.binaryWrittenBytes = 0;
                }

                // If there's more data available, process that
                that._processData(socket);
            }

            break;

        case SocketDataMode.BINARY:

            if (data) {
                msgdata.buffer = Buffer.concat([msgdata.buffer, data]);
            }

            var binaryDef = msgdata.binaryDef;

            if (msgdata.buffer.length) {

                if (msgdata.binaryWrittenBytes + msgdata.buffer.length <= msgdata.binaryLength) {
                    if (binaryDef['mode'] === BinaryType.FILE) {
                        msgdata.binaryFileStream.write(msgdata.buffer);
                    } else {
                        msgdata.buffer.copy(msgdata.binaryBuffer, msgdata.binaryWrittenBytes, 0, msgdata.buffer.length);
                    }
                    msgdata.binaryWrittenBytes += msgdata.buffer.length;
                    msgdata.buffer = new Buffer(0);
                } else {
                    var bytesToWrite = Math.min(msgdata.binaryLength - msgdata.binaryWrittenBytes, msgdata.buffer.length);
                    if (binaryDef['mode'] === BinaryType.FILE) {
                        msgdata.binaryFileStream.write(msgdata.buffer.slice(0, bytesToWrite));
                    } else {
                        msgdata.buffer.copy(msgdata.binaryBuffer, msgdata.binaryWrittenBytes, 0, bytesToWrite);
                    }
                    msgdata.binaryWrittenBytes += bytesToWrite;
                    msgdata.buffer = msgdata.buffer.slice(bytesToWrite)
                }
            }

            if (msgdata.binaryWrittenBytes === msgdata.binaryLength) {
                message = msgdata.pendingMessage;
                message.holdingUntilSend--;

                if (binaryDef['mode'] === BinaryType.FILE) {
                    msgdata.pendingMessage.files[binaryDef['key']] = {
                        'path': msgdata.binaryFilePath,
                        'name': binaryDef['name']
                    };
                    msgdata.binaryFileStream.end();
                } else {
                    var whereToPut = msgdata.pendingMessage.message;
                    var binaryKey = splitEscapedKeys(binaryDef['key']);
                    for (var i = 0; i < binaryKey.length - 1; i++) {
                        whereToPut = whereToPut[binaryKey[i]];
                    }
                    whereToPut[binaryKey[binaryKey.length - 1]] = msgdata.binaryBuffer;
                }

                if (message['binaries'].length) {
                    msgdata.mode = SocketDataMode.BINARY_LENGTH;
                } else {
                    msgdata.mode = SocketDataMode.PENDING;
                    delete msgdata.pendingMessage;
                }

                delete msgdata.binaryFilePath;
                delete msgdata.binaryFileStream;
                delete msgdata.binaryWrittenBytes;
                delete msgdata.binaryBuffer;
                delete msgdata.binaryDef;

                that._tryReleaseIncomingMessageQueue(socket);

                // If there's more data available, process that
                that._processData(socket);
            }

            break;

    }

};

/**
 * Queue an incoming message to be emitted
 * @private
 * @param {Socket} socket
 * @param {Object} message
 * @returns {Netmsg}
 */
Netmsg.prototype._queueIncomingMessage = function (socket, message) {
    var that = this;
    var msgdata = socket.__msgdata;

    msgdata.incomingMessages.push(message);

    return that._tryReleaseIncomingMessageQueue(socket);
};

/**
 * Try to dequeue a message to be emitted
 * @private
 * @param {Socket} socket
 * @returns {Netmsg}
 */
Netmsg.prototype._tryReleaseIncomingMessageQueue = function (socket) {
    var that = this;
    var msgdata = socket.__msgdata;

    while (msgdata.incomingMessages.length && !msgdata.incomingMessages[0].holdingUntilSend) {
        var message = msgdata.incomingMessages.shift();

        that.emit('message', {
            message: message['message']
            , files: message['files']
            , socket: socket
        });
    }

    return that;
};

/**
 * Queue a message to be sent out
 * @private
 * @param {Socket} socket
 * @param {Object} message
 * @param {Array} buffers
 * @param {Object} files
 * @returns {Netmsg}
 */
Netmsg.prototype._queueOutgoingMessage = function (socket, message, buffers, files) {
    var that = this;
    var msgdata = socket.__msgdata;

    msgdata.outgoingMessages.push({
        message: message,
        buffers: buffers,
        files: files,
        sending: false,
        holdingUntilSendComplete: buffers.length + files.length
    });

    return that._tryReleaseOutgoingMessageQueue(socket);
};

/**
 * Try to dequeue a message to be sent out
 * @private
 * @param {Socket} socket
 * @returns {Netmsg}
 */
Netmsg.prototype._tryReleaseOutgoingMessageQueue = function (socket) {
    var that = this;
    var msgdata = socket.__msgdata;

    var message = msgdata.outgoingMessages[0];
    if (!message) return that;

    if (message.sending) {
        // Try to send out pending files in a message

        if (message.pendingOutgoingFile) return that;

        if (!message.holdingUntilSendComplete) {
            msgdata.outgoingMessages.shift();
            return that._tryReleaseOutgoingMessageQueue(socket);
        }

        if (message.files.length) {

            var filePath = message.files.shift();
            message.pendingOutgoingFile = filePath;
            Fs.stat(filePath, function (error, stat) {
                if (error || !stat.isFile()) {
                    that.emit('warning', 'File with path "' + filePath + '" was not found');
                }

                var fileLength = (stat || {}).size || 0;

                if (socket.writable) {
                    var lengthBuffer = new Buffer(4);
                    lengthBuffer.writeUInt32LE(fileLength, 0);
                    socket.write(lengthBuffer);
                    if (fileLength <= 0) return;

                    var stream = message.pendingOutgoingStream =
                        Fs
                            .createReadStream(filePath)
                            .on('data', function (data) {
                                if (!socket.writable) {
                                    return stream.close();
                                }

                                socket.write(data);
                            })
                            .on('error', function (err) {
                                that.emit('error', err);
                                socket.destroy();
                            })
                            .on('end', function () {
                                message.holdingUntilSendComplete--;
                                delete message.pendingOutgoingStream;
                                delete message.pendingOutgoingFile;
                                //noinspection JSAccessibilityCheck
                                that._tryReleaseOutgoingMessageQueue(socket);
                            });
                }
            });

        }

        return that;
    }

    // Make sure that the socket is writable
    if (!socket.writable) {
        msgdata.outgoingMessages.length = 0;
        return that;
    }

    message.sending = true;

    var buffer = new Buffer(JSON.stringify(message.message));
    var lengthBuffer = new Buffer(4);
    lengthBuffer.writeUInt32LE(buffer.length, 0);
    socket.write(lengthBuffer);
    socket.write(buffer);

    // Send plain buffers
    for (var i = 0; i < message.buffers.length; i++) {
        buffer = message.buffers[i];
        lengthBuffer.writeUInt32LE(buffer.length, 0);
        socket.write(lengthBuffer);
        socket.write(buffer);
        message.holdingUntilSendComplete--;
    }

    // Start sending files
    if (message.files.length) {
        return that._tryReleaseOutgoingMessageQueue(socket);
    }

    if (!message.holdingUntilSendComplete) {
        msgdata.outgoingMessages.shift();
        that._tryReleaseOutgoingMessageQueue(socket);
    }

    return that;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Send a message to the other side
 * @param {Object} message The message to send. A message can contain `Buffer` objects.
 * @param {Object<String, {name,path}>?} files A map of files to send.
 * @returns {Netmsg}
 */
Netmsg.prototype.sendMessage = function (message, files) {
    return this.sendMessageTo(null, message, files);
};


//noinspection JSUnusedGlobalSymbols
/**
 * Send a message to the other side
 * @param {Socket?} socket The socket to send to, if you want to respond to a specific peer. Null to send to all.
 * @param {Object} message The message to send. A message can contain `Buffer` objects.
 * @param {Object<String, {name,path}>?} files A map of files to send.
 * @returns {Netmsg}
 */
Netmsg.prototype.sendMessageTo = function (socket, message, files) {
    var that = this;

    var i, buffersArray = [], filesArray = [];
    message = copyMessageExtractingBuffers(message == null ? {} : message, null, buffersArray);
    var binaries = [];

    for (i = 0; i < buffersArray.length; i++) {
        binaries.push({
            'mode': BinaryType.BUFFER
            , 'key': buffersArray[i].key
        });
        buffersArray[i] = buffersArray[i].buffer;
    }

    if (files) {
        for (var key in files) {
            if (!files.hasOwnProperty(key)) continue;

            var file = files[key];
            binaries.push({
                'mode': BinaryType.FILE
                , 'key': key
                , 'name': file['name']
            });
            filesArray.push(file['path']);
        }
    }

    if (socket) {
        that._queueOutgoingMessage(
            socket,
            {
                'message': message,
                'binaries': binaries.length ? binaries : false
            },
            buffersArray,
            filesArray
        );
    } else {
        for (i = 0; i < that._sockets.length; i++) {
            that._queueOutgoingMessage(
                that._sockets[i],
                {
                    'message': message,
                    'binaries': binaries ? binaries : false
                },
                buffersArray,
                filesArray
            );
        }
    }

    return that;
};

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
Netmsg.prototype.listen = function (options) {
    var that = this;

    var server = Net.createServer(function (socket) {
        that.listenOnSocket(socket);

        that.emit('connect', socket);
    })

    server.on('error', function (err) {
        // Forward errors
        that.emit('error', err);
    });

    that._listenerServers.push(server);

    server.listen(options);

    return that;
};

/**
 * Stop listening on a specific socket
 * @param {Socket} socket Socket to listen on
 * @returns {Netmsg}
 */
Netmsg.prototype.stopListeningOnSocket = function (socket) {
    var that = this;

    var i = that._sockets.indexOf(socket);
    if (i > -1) {
        that._sockets.splice(i, 1);
    }

    socket.removeListener('data', that._onData);
    socket.removeListener('end', that._onEnd);
    socket.removeListener('error', that._onError);
    socket.removeListener('close', that._onClose);

    return that;
};

/**
 * Start listening on a specific socket
 * @param {Socket} socket Socket to listen on
 * @returns {Netmsg}
 */
Netmsg.prototype.listenOnSocket = function (socket) {
    var that = this;

    that.stopListeningOnSocket(socket);

    socket.__msgdata = {
        buffer: new Buffer(0)
        , mode: SocketDataMode.PENDING
        , incomingMessages: []
        , outgoingMessages: []
    };

    that._sockets.push(socket);

    socket.on('data', that._onData);
    socket.on('end', that._onEnd);
    socket.on('error', that._onError);
    socket.on('close', that._onClose);

    return that;
};

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
Netmsg.prototype.connect = function (options) {

    var that = this;

    var socket = null;

    var retryCount = options.retry || 0;

    var timeoutMs = 
        (options.timeout && isFinite(options.timeout) && options.timeout > 0) 
        ? options.timeout 
        : 0;
    var totalTimeoutMs = 
        (options.totalTimeout && isFinite(options.totalTimeout) && options.totalTimeout > 0) 
        ? options.totalTimeout 
        : 0;
    
    var timeout = null;

    // Abort any previous `connect` operation
    if (that._connectingSocket) {
        var error = new Error();
        error.code = 'ECANCELED';
        that._connectingSocket.destroy(error);
        delete that._connectingSocket;
    }

    var connect = function () {
        if (timeoutMs) {
            timeout = setTimeout(function () {
                timeout = null;
                if (socket.connecting) {
                    var error = new Error();
                    error.code = 'ETIMEOUT';
                    socket.destroy(error); // This emits both ETIMEOUT and ECANCELED
                }
            }, timeoutMs);
        }

        socket = new Net.Socket({ allowHalfOpen: true })
            .on('error', onError)
            .on('connect', onConnect);

        var noDelay = options.noDelay === undefined ? true : options.noDelay;
        socket.setNoDelay(!!noDelay);

        socket.connect(options);

        that._connectingSocket = socket;
    };

    var onError = function (err) {
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
    };

    var onConnect = function (err) {

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
    };

    // Start connection attempt
    connect();

    // Set global timeout
    if (totalTimeoutMs) {
        var totalTimeout = setTimeout(function () {
            retryCount = 0;
            if (socket.connecting) {
                var error = new Error();
                error.code = 'ETIMEOUT';
                socket.destroy(error); // This emits both ETIMEOUT and ECANCELED
            }
        }, totalTimeoutMs);
    }

    return that;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Disconnect client from a remote server
 * @returns {Netmsg}
 */
Netmsg.prototype.disconnectClient = function () {

    var that = this;

    if (that._clientSocket) {
        that._clientSocket.destroy();
        that.stopListeningOnSocket(that._clientSocket);
    }

    return that;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Disconnect all sockets
 * @returns {Netmsg}
 */
Netmsg.prototype.disconnectAll = function () {

    var that = this;

    var sockets = that.getAllSockets();

    for (var i = 0; i < sockets.length; i++) {
        sockets[i].destroy();
        that.stopListeningOnSocket(sockets[i]);
    }

    return that;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Retrieve and array of all sockets.
 * The array is *not* a reference to an internal array.
 * @returns {Array.<Socket>}
 */
Netmsg.prototype.getAllSockets = function () {
    return this._sockets.slice(0);
};

/** @type {Netmsg} */
module.exports = Netmsg;

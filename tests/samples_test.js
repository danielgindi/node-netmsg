const assert = require('assert');
const Netmsg = require('../');

describe('End to end', async () => {

    it(`Test end to end - with simple json data and a Buffer`, async () => {
        return new Promise((resolve, reject) => {
            let server = new Netmsg();
            let client = new Netmsg();

            let SAMPLE_DATA = {
                val1: 123,
                val2: 123.456,
                val3: 'Some string',
                val4: Buffer.from('Some binary data: \x01\x02\x03\x04\x05')
            };

            let timeout = setTimeout(() => {
                onError(new Error('Did not complete test in a reasonable time'));
            }, 100);

            function onError(ex) {
                server.stopServer();
                server.disconnectAll();
                client.disconnectAll();

                clearTimeout(timeout);
                reject(ex);
            }

            let receivedMessageIndex = 0;
            let countToReceive = 3;

            client
                .on('error', onError)
                .on('connect', () => {
                    client.sendMessage(Object.assign({}, SAMPLE_DATA, {index: 0}));
                    client.sendMessage(Object.assign({}, SAMPLE_DATA, {index: 1}));
                    client.sendMessage(Object.assign({}, SAMPLE_DATA, {index: 2}));
                });

            server
                .listen({
                    host: '127.0.0.1',
                    port: 54321
                })
                .on('error', onError)
                .on('listening', () => {
                    client
                        .connect({
                            host: '127.0.0.1',
                            port: 54321
                        });
                })
                .on('message', message => {
                    countToReceive--;

                    assert.deepStrictEqual(
                        message.message,
                        Object.assign({}, SAMPLE_DATA, {index: receivedMessageIndex}),
                        'Arrived message is incorrect');

                    receivedMessageIndex++;

                    if (countToReceive === 0) {
                        clearTimeout(timeout);
                        server.stopServer();
                        server.disconnectAll();
                        client.disconnectAll();

                        resolve();
                    }
                });

        });
    });

});

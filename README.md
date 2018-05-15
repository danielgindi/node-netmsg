# netmsg

[![npm Version](https://badge.fury.io/js/netmsg.png)](https://npmjs.org/package/netmsg)

Simple message transfer over network between Node.js, with Buffer and file support

## Installation:

```
npm install --save netmsg
```
  
## Usage example:

```javascript

const Netmsg = require('netmsg');

let server = new Netmsg().listen({ host: '0.0.0.0', port: 1974 });

server.on('message', function (event) {
    console.log(event.message); // --> { "note": "this message has a file attached to it" }
    console.log(event.files); // --> { "file1": { "name": "calc.exe", "path": "C:\Windows\Temp\1-4345fdgk20asdnvbc.tmp" } }

    this.sendMessage({
        'some_data': 'that I have to send'
        , 'my_buffer': new Buffer([0x23, 0x73, 0xa0, 0xbf])
    });
});


let client = new Netmsg().connect({ host: '10.0.0.27', port: 1974 });
client.on('connect', function () {
    client.sendMessage({
        'note': 'this message has a file attached to it'
    }, {
        'file1': {
            'name': 'calc.exe'
            , 'path': 'C:\\Windows\\Temp\\1-4345fdgk20asdnvbc.tmp'
        }
    })
});
client.on('message', function (event) {
    console.log(event.message); // --> { "some_data": "that I have to send", "my_buffer": <Buffer 23 73 a0 bf> }
});

```

## API

* The constructor takes no arguments.
* Call `sendMessage(message, files)` to send a message
* Call `sendMessageTo(socket, message, files)` to send a message to a specific peer
* Call `listen(options)` to start a server
* Call `connect(options)` to connect to a server
* Call `stopListeningOnSocket(socket)` to remove a specific socket from this instance
* Call `disconnectClient()` to disconnect from the remote server, after calling `connect`
* Call `disconnectAll()` to disconnect all sockets, including server sockets and the client socket.
* Events that are emitted are: 'connect', 'disconnect', 'message', 'error'


## Contributing

If you have anything to contribute, or functionality that you lack - you are more than welcome to participate in this!
If anyone wishes to contribute unit tests - that also would be great :-)

## Me
* Hi! I am Daniel Cohen Gindi. Or in short- Daniel.
* danielgindi@gmail.com is my email address.
* That's all you need to know.

## Help

If you want to buy me a beer, you are very welcome to
[![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donate_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=G6CELS3E997ZE)
 Thanks :-)

## License

All the code here is under MIT license. Which means you could do virtually anything with the code.
I will appreciate it very much if you keep an attribution where appropriate.

    The MIT License (MIT)

    Copyright (c) 2013 Daniel Cohen Gindi (danielgindi@gmail.com)

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import {Client} from '@stomp/stompjs';

import {WebSocket} from 'ws';

Object.assign(global, {WebSocket});
const client = new Client({
    logRawCommunication: true,
    brokerURL: 'ws://tacocat:61616/topic', connectionTimeout: 1000, onConnect: () => {
        console.log("Connected")
        client.subscribe('SWT_TS_STORED', message => {
            console.log(`Received: ${message.body}`);
            message.ack();
        }, {ack: 'client'});
    }, onStompError: (frame) => {
        console.log('Broker reported error: ' + frame.headers['message']);
        console.log('Additional details: ' + frame.body);
    }
});

client.activate();
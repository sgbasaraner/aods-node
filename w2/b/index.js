#!/usr/bin/env node

const readline = require('readline');

let allMessages = new Set();
let msg_counter = 0;
let neighbors = [];

(async function () {
    for await (const line of readline.createInterface({ input: process.stdin })) {
        const msg = JSON.parse(line)
        switch (msg.body.type) {
            case "init":
                init(msg);
                break;
            case "broadcast":
                broadcast(msg)
                break;
            case "read":
                read(msg)
                break;
            case "topology":
                topology(msg)
                break;
        }


    }
})()

function init(msg) {
    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "init_ok"
        }
    }
    console.log(JSON.stringify(resp))
}

function broadcast(msg) {
    if (!allMessages.has(msg.body.message)) {
        allMessages.add(msg.body.message)

        neighbors.forEach(n => {
            const resp = {
                src: msg.dest,
                dest: n,
                body: {
                    type: "broadcast",
                    msg_id: ++msg_counter,
                    message: msg.body.message
                }
            }
            console.log(JSON.stringify(resp))
        });
    }

    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "broadcast_ok",
            msg_id: ++msg_counter,
        }
    }
    console.log(JSON.stringify(resp))
}

function read(msg) {
    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "read_ok",
            msg_id: ++msg_counter,
            messages: [...allMessages]
        }
    }
    console.log(JSON.stringify(resp))
}

function topology(msg) {
    neighbors = msg.body.topology[msg.dest]
    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "topology_ok",
            msg_id: ++msg_counter,
        }
    }
    console.log(JSON.stringify(resp))
}
#!/usr/bin/env node

const readline = require('readline');
const fs = require('fs');

let allMessages = new Set();
let msg_counter = 0;
let messageTargets = [];
let thisNodeId;
let pendingRetries = [];
let msgIdPendingRetryIdxMap = {};

setInterval(function () { processRetries() }, 400);

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
            case "broadcast_ok":
                broadcastReceipt(msg);
                break;
        }


    }
})()

function init(msg) {
    thisNodeId = msg.dest;
    buildTopology(msg.body.node_ids)
    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "init_ok"
        }
    }
    write(JSON.stringify(resp))
}

function broadcast(msg) {
    if (!allMessages.has(msg.body.message)) {
        allMessages.add(msg.body.message)

        messageTargets.forEach(n => {
            if (n !== msg.src) {
                pendingRetries.push([n, msg.body.message])
                const thismsgid = ++msg_counter;
                msgIdPendingRetryIdxMap[thismsgid] = pendingRetries.length - 1;
                const resp = {
                    src: msg.dest,
                    dest: n,
                    body: {
                        type: "broadcast",
                        msg_id: thismsgid,
                        message: msg.body.message
                    }
                }
                write(JSON.stringify(resp))
            }
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
    write(JSON.stringify(resp))
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
    write(JSON.stringify(resp))
}

function topology(msg) {
    const resp = {
        src: msg.dest,
        dest: msg.src,
        body: {
            in_reply_to: msg.body.msg_id,
            type: "topology_ok",
            msg_id: ++msg_counter,
        }
    }
    write(JSON.stringify(resp))
}

function buildTopology(allNodeIds) {
    const groups = [[]]
    const groupSize = 3;
    
    let isParent = allNodeIds[0] === thisNodeId;
    let groupIdx = 0;
    for (let i = 0; i < allNodeIds.length; i++) {
        const iterNode = allNodeIds[i];
        if (groups[groups.length - 1].length < groupSize) {
            groups[groups.length - 1].push(iterNode);
        } else {
            const newGroup = [iterNode]
            groups.push(newGroup)
            if (iterNode === thisNodeId) {
                isParent = true;    
            }
        }
        if (iterNode === thisNodeId) {
            groupIdx = groups.length - 1;
        }
    }

    if (isParent) {
        messageTargets.push(...(groups[groupIdx]))
        messageTargets.push(...(groups.map((g) => g[0])))
        messageTargets = messageTargets.filter((n) => n !== thisNodeId)
    } else {
        messageTargets = [(groups[groupIdx][0])]
    }
    process.stderr.write(`n: ${thisNodeId}, t: ${messageTargets.join(", ")}`)
}

function broadcastReceipt(msg) {
    const receivedMsgId = msg.body.in_reply_to;
    pendingRetries.splice(msgIdPendingRetryIdxMap[receivedMsgId], 1);
    delete msgIdPendingRetryIdxMap[receivedMsgId];
}

function processRetries() {
    const retriesThisRound = []
    pendingRetries.forEach(([n, msg]) => {
        const thismsgid = ++msg_counter;
        const resp = {
            src: thisNodeId,
            dest: n,
            body: {
                type: "broadcast",
                msg_id: thismsgid,
                message: msg
            }
        }
        write(JSON.stringify(resp))
        retriesThisRound.push([n, msg, thismsgid])
    })
    pendingRetries = []
    msgIdPendingRetryIdxMap = {};
    retriesThisRound.forEach(([n, msg, msgid]) => {
        pendingRetries.push([n, msg])
        msgIdPendingRetryIdxMap[msgid] = pendingRetries.length - 1;
    });
}

function write(msg) {
    fs.writeSync(1, msg + "\n");
}
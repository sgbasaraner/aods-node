#!/usr/bin/env node

const readline = require('readline');
const crypto = require('crypto');

(async function () {
    for await (const line of readline.createInterface({ input: process.stdin })) {
        const msg = JSON.parse(line)
    
        let resp;
        let msg_counter = 0;
        switch (msg.body.type) {
            case "init":
                resp = {
                    src: msg.dest,
                    dest: msg.src,
                    body: {
                        in_reply_to: msg.body.msg_id,
                        type: "init_ok"
                    }
                }
                break;
            default:
                resp = {
                    src: msg.dest,
                    dest: msg.src,
                    body: {
                        in_reply_to: msg.body.msg_id,
                        type: "generate_ok",
                        msg_id: ++msg_counter,
                        id: crypto.randomUUID()
                    }
                }
                break;
        }

        console.log(JSON.stringify(resp))
    }
})()
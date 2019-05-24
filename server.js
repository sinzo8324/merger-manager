require('dotenv-expand')(require('dotenv').config({ path: require('path').join(__dirname, '/.env') }));
const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const packer = require("./utils/packer.js");
const fs = require('fs');

const LOAD_ARGS = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
};

const PROTO_PATH = __dirname + '/protos/'+ process.env.PROTO_FILE;
const JSON_PATH = __dirname + '/' + process.env.MERGER_LIST_FILE;

const rawdata = fs.readFileSync(JSON_PATH);
const mergerList = JSON.parse(rawdata);

const packageDefinition = protoLoader.loadSync(
    PROTO_PATH, LOAD_ARGS
);
//Streaming channel signer <-> m/m
let users = new Map();
//GRPC Signer Service Object m/m <-> merger
let mergers = new Map();

const mmID = new Buffer(process.env.MERGER_MANAGER_ID).toString('base64');
const protoSigner = grpc.loadPackageDefinition(packageDefinition).grpc_signer;

for(let i = 0; i < mergerList.length; i++) {
    const address = mergerList[i].address + ':' + mergerList[i].port;
    const msg_sender = new protoSigner.GruutSignerService(address, grpc.credentials.createInsecure());
    mergers.set(mergerList[i].id, msg_sender);
}


/**
 * Open Streaming Channel (m/m <-> merger)
 * targetID = Merger ID wants to connect
 */
function openChannelToMerger(targetID){
    const identify = packer.protobuf_id_serializer(PROTO_PATH, "grpc_signer.Identity", new Buffer.from(mmID));
    const msg_sender = mergers.get(targetID);
    const channel = msg_sender.openChannel();
    channel.write(identify);
    channel.on("data", reply => {
        let payload = packer.unpack(reply.message);
        const header = packer.getHeader(reply.message);
        const sID = payload.sID;
        const mID = payload.mID;
        delete payload["sID"];

        const sIDs = JSON.parse(JSON.stringify(sID));
        const replyMsg = packer.pack(header.msg_type, payload, header.chainid, header.sender);
        const Msg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.ReplyMsg", replyMsg);
        let connectionList;

        if(Array.isArray(sIDs)){
            for(let i = 0; i < sIDs.length; i++){
                connectionList = users.get(sIDs[i]);
                try{
                    connectionList.get(mID).write(Msg);
                } catch(e) {
                    sendLeaveMsg(sIDs[i], mID);
                }
            }
        } else {
            connectionList = users.get(sIDs);
            try{
                connectionList.get(mID).write(Msg);
            } catch(e) {
                sendLeaveMsg(sIDs, mID);
            }
        }
    });
    
    channel.on("end", () => {
        console.log("The channel has closed by the server");
        });
    channel.on("error",  () => {
        console.log("The server is DEAD");
        });
}

/**
 * rpc OpenChannel (Signer <-> m/m)
 */
function doOpenChannel(call) {
    call.on('data', msg => {
        const sID = msg.sender.toString('ascii');
        const mID = msg.receiver.toString('ascii');
        let connectionList = users.get(sID);

        if(connectionList != null){
            if(connectionList.get(mID) != null){
                sendLeaveMsg(sID, mID);
                connectionList.get(mID).end();
            }
            connectionList.set(mID, call);
        }else {
            connectionList = new Map();
            connectionList.set(mID, call);
            users.set(sID, connectionList);
        }
    });

    call.on('end', () =>{
        let iterator = users.entries();
        let entry = iterator.next();
        while(entry.value != null){
            let connectionList = entry.value[1];
            const mID = [...connectionList.entries()]
                        .filter(({ 1: v }) => v === call)
                        .map(([k]) => k);
            if(mID != null){
                sendLeaveMsg(entry.value[0], mID[0]);
                connectionList.delete(mID[0]);
            }
            entry = iterator.next();
        }
        call.end();
    });
    call.on("error",  () => {
        console.log("The server is DEAD");
    });
}

/**
 * rpc signerService (signer <-> m/m)
 */
function doSignerService(msg, sendRes) {
    const header = packer.getHeader(msg.request.message);
    if(header.msg_type == packer.MSG_TYPE.MSG_GET_MERGER_ID){
        console.log("get merger ID list")
        let response = {};
        response.status = "SUCCESS";
        let mIDs = {};
        mIDs.id = new Array();
        for(let i = 0; i < mergerList.length; i++){
            mIDs.id.push(mergerList[i].id);
        }
        response.message = JSON.stringify(mIDs);
        sendRes(null, response);
    } else {
        let payload = packer.unpack(msg.request.message);
        const msg_sender = mergers.get(payload.mID);
        delete payload["mID"];
        payload.mmID = mmID;
        const req = packer.pack(header.msg_type, payload, header.chainid, header.sender);
        const reqMsg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.RequestMsg", req);

        msg_sender.signerService(reqMsg, (err, res) => {
            sendRes(err, res);
        });
    }
}

/**
 * Send Leave Message to merger
 */
function sendLeaveMsg(sID, mID){
    let msg = {};
    msg.sID = sID;
    msg.time = packer.getTimestamp();
    msg.msg = "disconnected with signer";
    const dummyCID = new Buffer("DUMMYCID").toString('base64');
    const req = packer.pack(packer.MSG_TYPE.MSG_LEAVE, msg, dummyCID, sID);
    const reqMsg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.RequestMsg", req);
    const msg_sender = mergers.get(mID);
    if(msg_sender != null){
        msg_sender.signerService(reqMsg, ()=>{
            console.log("Send Leave Msg : " + sID);
        });
    }
}


/**
 * @return {!Object} gRPC server
 */
function getServer() {
  const server = new grpc.Server();
  server.addService(protoSigner.GruutSignerService.service, {
    openChannel: doOpenChannel,
    signerService: doSignerService
  });
  return server;
}

if (require.main === module) {
    for(let i = 0; i < mergerList.length; i++){
        openChannelToMerger(mergerList[i].id);
    }
    const server = getServer();
    const addr = '0.0.0.0:' + process.env.PORT;
    server.bind(addr, grpc.ServerCredentials.createInsecure());
    server.start();
}

exports.getServer = getServer;
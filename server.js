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

const mergerList = readMergerList(JSON_PATH);

const packageDefinition = protoLoader.loadSync(
    PROTO_PATH, LOAD_ARGS
);

let grpcConnections = new Map();
let signerStreamingList = new Map();

const protoSigner = grpc.loadPackageDefinition(packageDefinition).grpc_signer;

function readMergerList(filePath){
    let mergerList;
    try{
        const rawdata = fs.readFileSync(filePath);
        mergerList = JSON.parse(rawdata);
    }catch(e){
        console.log("error");
    }
    return mergerList;
}

function getIdx(mergerID){
    for(let i = 0; i < Object.keys(mergerList).length; i++){
        if(mergerList[i].id === mergerID){
            return i;
        }
    }
    return null;
}

/**
 * Send Leave Message to merger
 */
function sendLeaveMsg(connection, sID){
    let msg = {};
    msg.sID = sID;
    msg.time = packer.getTimestamp();
    msg.msg = "disconnected with signer";
    const dummyCID = new Buffer("DUMMYCID").toString('base64');
    const req = packer.pack(packer.MSG_TYPE.MSG_LEAVE, msg, dummyCID, sID);
    const reqMsg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.RequestMsg", req);
    connection.signerService(reqMsg, ()=>{
        console.log("Send Leave Msg : " + sID);
    });
}

function setSignerStreamingChannel(sID, mID, call){
    let channelList = signerStreamingList.get(sID);
    if(channelList == null){
        channelList = new Map();
        signerStreamingList.set(sID, channelList);
    }
    channelList.set(mID, call);
}

/**
 *  rpc Streaming channel
 */
function doOpenChannel(call) {
    call.on('data', msg => {
        const sID = msg.sender.toString('ascii');
        const mID = msg.receiver.toString('ascii');
        let connectionList = grpcConnections.get(sID);
        let connection;

        setSignerStreamingChannel(sID, mID, call);

        if(connectionList == null){
            connectionList = new Map();
            grpcConnections.set(sID, connectionList);
        }

        connection = connectionList.get(mID);
        if(connection != null){
            sendLeaveMsg(connection, sID);
        }

        const idx = getIdx(mID);
        const address = mergerList[idx].address + ':' + mergerList[idx].port;
        connection = new protoSigner.GruutSignerService(address, grpc.credentials.createInsecure());
        connectionList.set(mID, connection);
        const streamingChannel = connection.openChannel();
        streamingChannel.write(msg);

        streamingChannel.on("data", reply => {
            call.write(reply);
        });

        streamingChannel.on("end", () => {
            console.log("lost connection with merger");
            streamingChannel.end();
        });
        streamingChannel.on("error", () => {
            console.log("merger connection error");
            call.end();
        });
    });

    call.on('end', () =>{
        let iterator = signerStreamingList.entries();
        let entry = iterator.next();
        while(entry.value != null){
            let channelList = entry.value[1];
            const mID = [...channelList.entries()]
                        .filter(({ 1: v }) => v === call)
                        .map(([k]) => k);
            if(mID[0] !== undefined){
                let connectionToClose = (grpcConnections.get(entry.value[0])).get(mID[0]);
                sendLeaveMsg(connectionToClose, entry.value[0]);
                channelList.delete(mID[0]);
            }
            entry = iterator.next();
        }
        call.end();
    });
    call.on("error",  () => {
        console.log("signer connection error");
        call.end();
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
        const connectionList = grpcConnections.get(payload.sID);
        if(connectionList == null){
            console.log("Can not found Connection list of signer(" + payload.sID + ")");
            return;
        }
        const mID = payload.mID;
        const sender = connectionList.get(mID);
        if(sender == null){
            console.log("Can not found connection : signer(" + payload.sID + ") - merger(" + mID +")");
            return;
        }

        delete payload["mID"];

        const req = packer.pack(header.msg_type, payload, header.chainid, header.sender);
        const reqMsg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.RequestMsg", req);

        sender.signerService(reqMsg, (err, res) => {
            sendRes(err, res);
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
    const server = getServer();
    const addr = '0.0.0.0:' + process.env.PORT;
    server.bind(addr, grpc.ServerCredentials.createInsecure());
    server.start();
}

exports.getServer = getServer;
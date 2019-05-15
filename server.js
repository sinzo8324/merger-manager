require('dotenv-expand')(require('dotenv').config({ path: require('path').join(__dirname, '/.env') }));
const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const packer = require("./utils/packer.js");
const MERGER_ADDR = "10.193.1.50:50000";

const LOAD_ARGS = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
};

const PROTO_PATH = __dirname + '/protos/'+ process.env.PROTO_FILE;

const packageDefinition = protoLoader.loadSync(
    PROTO_PATH, LOAD_ARGS
);

let users = new Map();

const mmID = new Buffer(process.env.MERGER_MANAGER_ID).toString('base64');
const protoSigner = grpc.loadPackageDefinition(packageDefinition).grpc_signer;
//send msg from m/m  to merger
const msg_sender = new protoSigner.GruutSignerService(MERGER_ADDR, grpc.credentials.createInsecure());
let channel;

function openChannelToMerger(){
    const identify = packer.protobuf_id_serializer(PROTO_PATH, "grpc_signer.Identity", new Buffer.from(mmID));
    channel = msg_sender.openChannel();
    channel.write(identify);
    channel.on("data", reply => {
        let payload = packer.unpack(reply.message);
        const header = packer.getHeader(reply.message);
        const sID = payload.sID;
        delete payload["sID"];
        console.log("merger -> signer : " + sID);
        console.log(payload);

        const replyMsg = packer.pack(header.msg_type, payload, header.chainid, header.sender);
        const Msg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.ReplyMsg", replyMsg);
        users.get(sID).write(Msg);
        });
    
    channel.on("end", () => {
        console.log("The channel has closed by the server");
        });
    channel.on("error",  () => {
        console.log("The server is DEAD");
        });
}

/**
 * @param {!Object} call
 */
function doOpenChannel(call) {
  call.on('data', msg => {
      const sID = msg.sender.toString('ascii');
      console.log(msg);
      if(users.get(sID) != null){
        // request deletion of signer information which has the sID
        users.get(sID).end();
      }
      users.set(sID, call);
  });

  call.on('end', () =>{
      console.log('end');
      call.end();
  });
  call.on("error",  () => {
    console.log("The server is DEAD");
  });
}

/**
 * @param {!Object} msg
 */
function doSignerService(msg, sendRes) {
    console.log("signer -> merger");
    let payload = packer.unpack(msg.request.message);
    const header = packer.getHeader(msg.request.message);
    payload.mmID = mmID;
    console.log(payload);
    console.log(header);
    const req = packer.pack(header.msg_type, payload, header.chainid, header.sender);
    const reqMsg = packer.protobuf_msg_serializer(PROTO_PATH, "grpc_signer.RequestMsg", req);

    msg_sender.signerService(reqMsg, function(err, res){
       console.log(res);
       sendRes(err, res);
    });
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
    openChannelToMerger();
    const server = getServer();
    const addr = '0.0.0.0:' + process.env.PORT;
    server.bind(addr, grpc.ServerCredentials.createInsecure());
    server.start();
}

exports.getServer = getServer;
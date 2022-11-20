// load required packages
const logger = require("elogger");
const uuid = require("uuid");
const fs = require("fs");
const fsp = require('fs').promises;
const path = require('path');
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
// load file_uploader.proto to load the gRPC data contract
const packageDefinition = protoLoader.loadSync("./file_uploader.proto", {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});
const fileUploaderProto = grpc.loadPackageDefinition(packageDefinition).FileUploaderPackage;
// define file upload method
const endFile = (call, callback) => {
    logger.debug(`gRPC ${call.call.handler.path}`);

    var name
    // handle incoming data stream
    call.on('data', async (payload) => {
        console.log("endFile", payload)

        name = payload.name;
        payload.hash_blocks.map(e => {
            const dirPath = path.join(__dirname, '/tmp',e);
            console.log()

            fsp.readFile(`./tmp/${e}.bin`).then(data => {
                fs.appendFileSync(`./tmp/${name}`, data);
            });
            //fs.unlinkSync(`./tmp/${e}.bin`);
            logger.debug(e, `./tmp/${name}`);
        })

    });
    // on stream end send final response to the client with required details
    call.on('end', async () => {
        callback(null, {
            'path': `./tmp/${name}`,
        });
    });
};

const uploadBlock = (call, callback) => {
    logger.debug(`gRPC ${call.call.handler.path}`);

    let hash
    let index = 0
    // handle incoming data stream
    call.on('data', async (payload) => {
        if (payload.data && payload.data == 'hash' && payload[payload.data]) {
            hash = payload[payload.data];
        }
        else if (payload.data && payload.data == 'chunk' && payload[payload.data]) {
            fs.appendFileSync(`./tmp/${hash}.bin`, payload[payload.data]);
            index++
            logger.debug("uploadBlock", hash, index);
        }
    });
    // on stream end send final response to the client with required details
    call.on('end', async () => {
        callback(null, {
            'id': index,
        });
    });
};

// initialize server and register handlers for the respective RPC methods
const server = new grpc.Server();
server.addService(fileUploaderProto.FileUploaderService.service, {
    uploadBlock: uploadBlock,
    endFile: endFile
});
// bind & start the server process to port: 9090
const bindEndpoint = `0.0.0.0:9090`;
server.bindAsync(bindEndpoint, grpc.ServerCredentials.createInsecure(), (err, response) => {
    if (err) {
        logger.error(err);
    }
    else {
        server.start();
        logger.info(`File uploader gRPC service started on grpc://${bindEndpoint}`);

    }
})
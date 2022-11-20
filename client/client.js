// load required packages
const fs = require('fs');
const fsPromises = fs.promises;
var crypto = require('crypto');
const logger = require('elogger');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
// load file_uploader.proto to load the gRPC data contract
const packageDefinition = protoLoader.loadSync('./file_uploader.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});
const endpoint = 'localhost:9090';
const fileUploaderProto = grpc.loadPackageDefinition(packageDefinition).FileUploaderPackage;
const serviceStub = new fileUploaderProto.FileUploaderService(endpoint, grpc.credentials.createInsecure());

const size_block = 100
const file_path = './test.txt'
// achar tamanho do arquivo
var stats = fs.statSync(file_path)
var filehandle = null;

const n_blocks = Math.round(stats.size / size_block)
console.log("n_blocks", n_blocks, stats.size)

var buff_blocks = new Array(n_blocks)

for (let i = 0; i <= n_blocks; i++) {
    buff_blocks[i] = new Buffer.alloc(size_block);
}

let hash_blocks = new Array(n_blocks)

async function main() {
    // Using the filehandle method
    filehandle = await fsPromises
        .open(file_path, 'r+');


    let promises = []
    let position = 0;
    for (let i = 0; i < n_blocks; i++) {
        position = Math.round(i * size_block)
        promises.push(send(i, size_block, position))
    }

    let last_position = Math.round(n_blocks * size_block)
    let last_size = stats.size - last_position - 1

    promises.push(send(n_blocks, last_position, last_size))

    await Promise.all(promises).finally(async () => {
        console.log("finally")

        // Close the file if it is opened.
        await filehandle.close();

        // FIM do upload

        // call service stub method and add callback to get the final response
        const serviceCall = serviceStub.endFile((err, response) => {
            if (err) {
                logger.error(err);
            }
            else {
                console.log(response);
            }
        });
        // write the stream data to the service stub method instance
        serviceCall.write({
            name: 'test.txt',
            hash_blocks: hash_blocks
        });

        serviceCall.end();

    })
}

async function send(i, size_block, position) {
    return new Promise(async (resolve, reject) => {

        console.log("block", i, size_block, position)
        // Calling the filehandle.read() method
        try {
            await filehandle.read(buff_blocks[i],
                0, size_block, position);


            hash_blocks[i] = crypto.createHash("sha512").update(buff_blocks[i]).digest("hex");


            // call service stub method and add callback to get the final response
            const serviceCall = serviceStub.uploadBlock((err, response) => {
                if (err) {
                    logger.error(err);
                }
                else {
                    console.log(i, response);
                }
            });
            // write the stream data to the service stub method instance
            serviceCall.write({
                hash: hash_blocks[i]
            });

            console.log("buf", i, buff_blocks[i].toString());
            serviceCall.write({
                chunk: Uint8Array.from(buff_blocks[i])
            });

            serviceCall.end(() => resolve());
        } catch (err) {
            console.log("send", err)
        }


    })
}

main()
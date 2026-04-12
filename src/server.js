import express from "express";

import http from "http";

const app = express();
const server = http.createServer(app);

server.listen(3002,"https://URL",()=>{
    console.log('Server is running');
})
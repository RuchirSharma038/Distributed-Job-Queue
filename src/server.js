import express from "express";
import { createServer } from 'http';
import { Server as SocketServer } from 'socket.io';
import { logger } from './config/logger.js';
import { setIo } from './config/socket.js';
import { startSubscriber } from './config/redisSubscriber.js';
import jobRoutes from './api/routes/job.routes.js';
import systemRoutes from './api/routes/system.routes.js'
const PORT = process.env.PORT || 3002;

const app = express();

app.use(express.json());
app.use('/api', jobRoutes);
app.use('/api/system', systemRoutes);
app.use('/static', express.static('public'));
//const server = http.createServer(app);

const httpServer = createServer(app);

const io = new SocketServer(httpServer, {
    cors: {

        origin: '*',
    }
});

setIo(io);

io.on('connection', (socket) => {
    logger.info({ socketId: socket.id }, 'Dashboard client connected');


    socket.emit('connected', {
        message: 'Connected to Job Queue real-time feed',
        timestamp: new Date().toISOString(),
    });

    socket.on('disconnect', (reason) => {
        logger.info({ socketId: socket.id, reason }, 'Dashboard client disconnected');
    });
});


startSubscriber();


httpServer.listen(PORT, () => {
    logger.info({ port: PORT }, `API + WebSocket server running on http://localhost:${PORT}`);
});
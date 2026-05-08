import express from "express";

// import http from "http";

import jobRoutes from './api/routes/job.routes.js';
const PORT = 3002;

const app = express();

app.use(express.json());
app.use('/api',jobRoutes);
app.use('/static', express.static('public'));
//const server = http.createServer(app);

app.listen(PORT,  () => {
    console.log(`API Producer Server running on http://localhost:${PORT}`);
});
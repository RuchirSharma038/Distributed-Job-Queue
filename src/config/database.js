// src/config/database.js
const { PrismaClient } = require('@prisma/client');
const { PrismaPg } = require('@prisma/adapter-pg');
const { Pool } = require('pg');

// 1. Create the standard Postgres connection pool (Just like Week 1!)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5 // Strict connection limit for when PM2 spawns multiple workers
});

// 2. Wrap the pool in the Prisma Adapter
const adapter = new PrismaPg(pool);

// 3. Pass the adapter into the Prisma Client
const prisma = new PrismaClient({ adapter });

module.exports = prisma;
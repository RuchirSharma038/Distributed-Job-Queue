import 'dotenv/config';
import pkg from '@prisma/client';

import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';

const { PrismaClient } = pkg;
const { Pool } = pg;

//  Create the standard Postgres connection pool 
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5 // connection limit 
});

//  Wrap the pool in the Prisma Adapter
const adapter = new PrismaPg(pool);

//  Pass the adapter into the Prisma Client
const prisma = new PrismaClient({ adapter });

export default prisma;
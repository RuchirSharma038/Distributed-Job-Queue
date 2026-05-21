import nodemailer from 'nodemailer';
import prisma from '../../config/database.js';
import redis from '../../config/redis.js';
//import { logger } from '../../config/logger.js';

const SENT_KEY_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

let transporter = null;

async function getTransporter(log) {
    if (transporter) return transporter;


    const testAccount = await nodemailer.createTestAccount();

    transporter = nodemailer.createTransport({
        host: 'smtp.ethereal.email',
        port: 587,
        secure: false,
        auth: {
            user: testAccount.user,
            pass: testAccount.pass,
        },
    });

    log.info({ account: testAccount.user }, "Ethereal SMTP initialized");
    return transporter;
}


// Idempotency helper 


async function isAlreadySent(jobId) {
    try {
        const val = await redis.get(`sent_email:${jobId}`);
        return val !== null;
    } catch {
        return false;
    }
}

async function markAsSent(jobId) {
    await redis.set(`sent_email:${jobId}`, '1', 'EX', SENT_KEY_TTL_SECONDS);
}


// Handler


const processEmail = async (payload, jobId, log) => {

    //const jobId = jobLogger.bindings().job_id;

    const { to, subject, body } = payload;
    log.info({ to, subject }, "Email handler started");

    // IDEMPOTENCY CHECK
    if (await isAlreadySent(jobId)) {
        log.info({ jobId }, "Idempotency: email already sent on a previous attempt — skipping");
        return { success: true, cached: true, message: 'Email already sent on a previous attempt' };
    }


    // Send the email

    const smtp = await getTransporter(log);

    const info = await smtp.sendMail({
        from: '"Job Queue System" <noreply@jobqueue.dev>',
        to,
        subject,
        text: body,
        html: `<p>${body}</p>`,
    });


    await markAsSent(jobId);

    const previewUrl = nodemailer.getTestMessageUrl(info);
    log.info({ messageId: info.messageId, previewUrl }, "Email dispatched and marked as sent");

    return {
        success: true,
        messageId: info.messageId,
        previewUrl,
    };
};

export default processEmail;
import nodemailer from 'nodemailer';
import prisma from '../../config/database.js';
import redis from '../../config/redis.js';

const SENT_KEY_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

let transporter = null;

async function getTransporter() {
    if (transporter) return transporter;

    // Ethereal auto-creates a test account — great for dev, zero config
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

    logger.info({ user: testAccount.user }, "Ethereal SMTP initialized");
    return transporter;
}


// Idempotency helper 


async function isAlreadySent(jobId) {
    const val = await redis.get(`sent_email:${jobId}`);
    return val !== null;
}

async function markAsSent(jobId) {
    await redis.set(`sent_email:${jobId}`, '1', 'EX', SENT_KEY_TTL_SECONDS);
}


// Handler


const processEmail = async (payload, jobId) => {

    const jobId = jobLogger.bindings().job_id;

    const { to, subject, body } = payload;

    jobLogger.info({ to }, "Initiating email send sequence");


    // IDEMPOTENCY CHECK

    if (await isAlreadySent(jobId)) {
        jobLogger.warn("Idempotency triggered: email already sent. Bypassing SMTP.");
        return { success: true, cached: true, message: 'Email already sent on a previous attempt' };
    }


    // Send the email

    const smtp = await getTransporter();

    const info = await smtp.sendMail({
        from: '"Job Queue System" <noreply@jobqueue.dev>',
        to,
        subject,
        text: body,
        html: `<p>${body}</p>`,
    });


    await markAsSent(jobId);

    const previewUrl = nodemailer.getTestMessageUrl(info);
jobLogger.info({ messageId: info.messageId, previewUrl }, "Email dispatched and marked as sent");

    return {
        success: true,
        messageId: info.messageId,
        previewUrl,
    };
};

export default processEmail;
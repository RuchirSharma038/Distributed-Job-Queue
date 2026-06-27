import nodemailer from 'nodemailer';
import prisma from '../../config/database.js';
import redis from '../../config/redis.js';
//import { logger } from '../../config/logger.js';

const PRE_SEND_TTL_S = 60;          // 1 minute 
const POST_SEND_TTL_S = 7 * 24 * 3600;

let transporter = null;
let transporterPromise = null;

async function getTransporter(log) {
    if (transporter) return transporter;


    if (!transporterPromise) {

        transporterPromise = (async () => {
            try {
                const account = await nodemailer.createTestAccount();

                transporter = nodemailer.createTransport({
                    host: 'smtp.ethereal.email',
                    port: 587,
                    auth: { user: account.user, pass: account.pass },
                });

                return transporter;
            } catch (err) {
                //  Reset THE PROMISE
                transporterPromise = null;
                throw err;
            }
        })();
    }
    return transporterPromise;
}



// Handler


const processEmail = async (payload, jobId, log) => {


    const { to, subject, body } = payload;
    const idempotencyKey = `sent_email:${jobId}`;

    // 
    const acquired = await redis.set(idempotencyKey, 'sending', 'NX', 'EX', PRE_SEND_TTL_S);

    if (!acquired) {

        log.info({ jobId, to }, 'Email already sent or in-flight — skipping (idempotent)');
        return { cached: true, jobId };
    }


    log.info({ to, subject }, "Email handler started");




    // Send the email

    const smtp = await getTransporter(log);

    let info;
    try {

        info = await smtp.sendMail({
            from: '"Job Queue System" <noreply@jobqueue.dev>',
            to,
            subject,
            text: body,
            html: `<p>${body}</p>`,
        });
    } catch (smtpErr) {
        log.warn({ jobId, err: smtpErr.message },
            'SMTP failed — deleting idempotency key so retry can attempt again');
        await redis.del(idempotencyKey);
        throw smtpErr;
    }


    await redis.set(idempotencyKey, 'sent', 'EX', POST_SEND_TTL_S);

    const previewUrl = nodemailer.getTestMessageUrl(info);
    log.info({ messageId: info.messageId, previewUrl }, "Email dispatched and marked as sent");

    return {
        success: true,
        messageId: info.messageId,
        previewUrl,
    };
};

export default processEmail;
import nodemailer from "nodemailer";




//Setup the transporter

const transporter = nodemailer.createTransport({
    host: 'smtp.ethereal.email',
    port: 587,
    auth: {
        user: process.env.ETHEREAL_USER ,
        pass: process.env.ETHEREAL_PASS
    }
});

//Delay for rate-limiting
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


const processEmail = async (payload) => {
    console.log(`[Email Handler] Processing email to: ${payload.to}`);

    //Throttle
    await sleep(800);

    try {
        const info = await transporter.sendMail({
            from: '"Job Queue API" <system@yourdomain.com>',
            to: payload.to,
            subject: payload.subject,
            text: payload.body
        });

        const previewUrl = nodemailer.getTestMessageUrl(info);
        console.log(`[Email Handler] Success! Preview here: ${previewUrl}`);

        return { success: true, messageId: info.messageId };

    } catch (error) {
        console.error(`[Email Handler] Failed: ${error.message}`);
        throw error;
    }
}
export default processEmail;
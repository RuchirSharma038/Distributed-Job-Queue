import PDFDocument from 'pdfkit';
import fs, { rename } from 'fs';
import fsp from 'fs/promises';
import path from 'path';

const OUTPUT_DIR = './public/pdfs';

async function isFileValid(filePath) {
    try {
        const stats = await fsp.stat(filePath);
        return stats.size > 0;
    } catch {
        return false;
    }
}

async function ensureDirectory(dir) {
    try {
        await fsp.access(dir);
    } catch {
        await fsp.mkdir(dir, { recursive: true });
    }
}

const processPdf = async (payload, jobId, log) => {
    const { filename, invoiceData } = payload;
    log.info({ filename }, "Starting PDF generation");

    await ensureDirectory(OUTPUT_DIR);

    const filePath = path.join(OUTPUT_DIR, filename);
    const tempPath = `${filePath}.tmp`;
    const publicUrl = `/static/pdfs/${filename}`;

    //IDEMPOTENCY CHECK
    if (await isFileValid(filePath)) {
        log.info({ filename }, "Idempotency: valid file already exists — returning cached URL");
        return { success: true, fileUrl: publicUrl, cached: true };
    }

    //Generate the PDF
    const doc = new PDFDocument({ margin: 50 });
    const writeStream = fs.createWriteStream(tempPath);

    doc.pipe(writeStream);

    doc.fontSize(25).font('Helvetica-Bold').text('INVOICE', { align: 'center' });
    doc.moveDown(2);

    doc.fontSize(14).font('Helvetica');
    doc.text(`Order ID: ${invoiceData.orderId || 'N/A'}`);
    doc.text(`Customer Name: ${invoiceData.customerName}`);
    doc.text(`Date: ${new Date().toLocaleDateString()}`);
    doc.moveDown(2);

    doc.font('Helvetica-Bold').text('Items Purchased:');
    doc.font('Helvetica');
    if (invoiceData.items && Array.isArray(invoiceData.items)) {
        invoiceData.items.forEach(item => {
            doc.text(`- ${item.name}: $${item.price}`);
        });
    }

    doc.moveDown();
    doc.font('Helvetica-Bold').text(`Total Amount: $${invoiceData.totalAmount}`);

    doc.end();

    // Wait for the stream to finish
    await new Promise((resolve, reject) => {
        writeStream.on('finish', async () => {
            try {
                await fsp.rename(tempPath, filePath);
                resolve();

            } catch (renameErr) {
                reject(renameErr);
            }
        });

        writeStream.on('error', async (err) => {

            try { await fsp.unlink(tempPath); } catch { /* ignore */ }
            log.error({ err: err.message }, "Stream error during PDF write");
            reject(err);
        });
    });
    log.info({ filePath, publicUrl }, "PDF generation completed");

    return { success: true, fileUrl: publicUrl, cached: false };


};

export default processPdf;
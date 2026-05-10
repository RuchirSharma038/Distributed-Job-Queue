import PDFDocument from 'pdfkit';
import fs from 'fs';
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

const processPdf = (payload) => {
    return new Promise((resolve, reject) => {
        try {

            const { filename, invoiceData } = payload;

            jobLogger.info({ filename }, "Starting PDF generation");

            await ensureDirectory(OUTPUT_DIR);

            const filePath = path.join(OUTPUT_DIR, filename);
            const publicUrl = `/static/pdfs/${filename}`;


            // IDEMPOTENCY CHECK

            if (await isFileValid(filePath)) {
                jobLogger.info("Idempotency: Valid file already exists — returning cached URL");
                return resolve({ success: true, fileUrl: publicUrl, cached: true });
            }


            // Generate the PDF

            const doc = new PDFDocument({ margin: 50 });
            const writeStream = fs.createWriteStream(filePath);

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

            writeStream.on('finish', () => {
                console.log(`[PDF Handler] Success — saved to ${filePath}`);
                resolve({ success: true, fileUrl: publicUrl, cached: false });
            });

            writeStream.on('error', (err) => {
                // Clean up the partial file so the next retry starts fresh

                try { await fsp.unlink(filePath); } catch { /* ignore */ }
                jobLogger.error({ err: err.message }, "Stream error during PDF write");
                reject(err);
            });

        } catch (error) {
            jobLogger.error({ err: error.message }, "PDF generation pipeline failed");

            if (error instanceof TypeError) {
                error.permanent = true;
            }
            reject(error);
        }
    });
};

export default processPdf;
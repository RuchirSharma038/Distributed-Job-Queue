import PDFDocument from 'pdfkit';
import fs from 'fs';
import path from 'path';

/**
 * Generates an Invoice PDF using PDFKit.
 * @param {Object} payload - Requires 'filename' and 'invoiceData'.
 */
const processPdf = (payload) => {
    return new Promise((resolve, reject) => {
        try {
            const { filename, invoiceData } = payload;
            
            console.log(`[PDF Handler] Generating document: ${filename}`);

            //  Define storage location
            const outputDir = './public/pdfs';
            
            // Ensure directory exists synchronously (only runs once if missing)
            if (!fs.existsSync(outputDir)) {
                fs.mkdirSync(outputDir, { recursive: true });
            }

            const filePath = path.join(outputDir, filename);

            //  Initialize PDFKit and the File Stream
            const doc = new PDFDocument({ margin: 50 });
            const writeStream = fs.createWriteStream(filePath);

            // Pipe the PDF generation directly into the file
            doc.pipe(writeStream);

            //  Draw the Document
            doc.fontSize(25).font('Helvetica-Bold').text('INVOICE', { align: 'center' });
            doc.moveDown(2);

            // Customer Details
            doc.fontSize(14).font('Helvetica');
            doc.text(`Order ID: ${invoiceData.orderId || 'N/A'}`);
            doc.text(`Customer Name: ${invoiceData.customerName}`);
            doc.text(`Date: ${new Date().toLocaleDateString()}`);
            doc.moveDown(2);

            // Line Items
            doc.font('Helvetica-Bold').text('Items Purchased:');
            doc.font('Helvetica');
            invoiceData.items.forEach(item => {
                doc.text(`- ${item.name}: $${item.price}`);
            });
            
            doc.moveDown();
            doc.font('Helvetica-Bold').text(`Total Amount: $${invoiceData.totalAmount}`);

            // Finalize the PDF
            doc.end();

            //  Wait for the file system to completely finish writing
            writeStream.on('finish', () => {
                console.log(`[PDF Handler] Success! Saved to ${filePath}`);
                
                // Return the URL path for Postgres 
                resolve({
                    success: true,
                    fileUrl: `/static/pdfs/${filename}` 
                });
            });

            // Handle file stream errors
            writeStream.on('error', (err) => {
                console.error(`[PDF Handler] Stream Error: ${err.message}`);
                reject(err);
            });

        } catch (error) {
            console.error(`[PDF Handler] Failed: ${error.message}`);
            reject(error);
        }
    });
};

export default processPdf;
import sharp from 'sharp';
import path from 'path';
import fs from 'fs/promises';
import crypto from 'crypto';

const OUTPUT_DIR = './public/processed';

function getOutputPath(jobId, operation) {
    const safeJobId = jobId.replace(/[^a-zA-Z0-9-_]/g, '');
    const map = {
        thumbnail: `${safeJobId}-thumb.jpg`,
        webp: `${safeJobId}.webp`,
        grayscale: `${safeJobId}-bw.jpg`,
    };
    if (!map[operation]) throw new Error(`Unknown operation: ${operation}`);
    return path.join(OUTPUT_DIR, map[operation]);
}

function getTempPath(jobId, operation) {
    const safeJobId = jobId.replace(/[^a-zA-Z0-9-_]/g, '');
    const token = crypto.randomBytes(4).toString('hex');
    return path.join(OUTPUT_DIR, `${safeJobId}-${operation}-${process.pid}-${token}.tmp`);
}

async function fileExists(filePath) {
    try {
        const stats = await fs.stat(filePath);

        return stats.size > 0;
    } catch {
        return false;
    }
}

async function atomicSharpWrite(pipeline, finalPath, tempPath) {
    let writeSucceeded = false;

    try {
        await pipeline.toFile(tempPath);
        writeSucceeded = true;
        await fs.rename(tempPath, finalPath);

    } catch (err) {
        if (writeSucceeded && (err.code === 'ENOENT' || err.code === 'EEXIST')) {

            return;
        }

        throw err;

    } finally {
        // Unconditional cleanup 
        try { await fs.unlink(tempPath); } catch { /* ENOENT = already moved */ }
    }
}

const processImage = async (payload, jobId, log) => {
    const { inputPath, filename, operations } = payload;
    log.info({ jobId, filename, operations }, 'Starting image processing');

    await fs.mkdir(OUTPUT_DIR, { recursive: true });

    // IDEMPOTENCY CHECK

    const existingResults = {};
    const pendingOperations = [];

    for (const op of operations) {
        const outputPath = getOutputPath(jobId, op);
        if (await fileExists(outputPath)) {

            existingResults[op] = `/processed/${path.basename(outputPath)}`;
            log.info({ jobId, operation: op }, "Idempotency: Output already exists and is valid, skipping");
        } else {
            pendingOperations.push(op);
        }
    }

    // All operations already completed 
    if (pendingOperations.length === 0) {
        log.info({ jobId }, "All outputs already exist — returning cached results");
        return { success: true, generatedFiles: existingResults, requestedFilename: filename };
    }


    // Source file check

    try {
        await fs.access(inputPath);
    } catch {

        if (Object.keys(existingResults).length > 0) {
            log.warn({ jobId }, "Source gone but partial results exist — returning what we have");
            return { success: true, generatedFiles: existingResults, requestedFilename: filename };
        }

        const err = new Error(`Source file not found at ${inputPath} and no outputs exist`);
        err.permanent = true;
        throw err;
    }


    // Process the pending operations

    const newResults = {};
    const imagePipeline = sharp(inputPath);

    try {


        if (pendingOperations.includes('thumbnail')) {
            const finalPath = getOutputPath(jobId, 'thumbnail');
            const tempPath = getTempPath(jobId, 'thumbnail');
            await atomicSharpWrite(
                imagePipeline.clone().resize(200, 200, { fit: 'cover' }).jpeg({ quality: 80 }),
                finalPath, tempPath
            );
            newResults.thumbnail = `/processed/${path.basename(finalPath)}`;
            log.info({ jobId, finalPath }, 'Thumbnail written');
        }

        if (pendingOperations.includes('webp')) {
            const finalPath = getOutputPath(jobId, 'webp');
            const tempPath = getTempPath(jobId, 'webp');
            await atomicSharpWrite(
                imagePipeline.clone().webp({ quality: 75 }),
                finalPath, tempPath
            );
            newResults.webp = `/processed/${path.basename(finalPath)}`;
            log.info({ jobId, finalPath }, 'WebP written');
        }

        if (pendingOperations.includes('grayscale')) {
            const finalPath = getOutputPath(jobId, 'grayscale');
            const tempPath = getTempPath(jobId, 'grayscale');
            await atomicSharpWrite(
                imagePipeline.clone().grayscale().jpeg({ quality: 90 }),
                finalPath, tempPath
            );
            newResults.grayscale = `/processed/${path.basename(finalPath)}`;
            log.info({ jobId, finalPath }, 'Grayscale written');
        }


        try {
            await fs.unlink(inputPath);
            log.info({jobId},"Cleaned up source file");
        } catch (cleanupError) {
            log.warn({ jobId, err: cleanupError.message }, "Failed to delete source file, but job succeeded. Proceeding.");
        }

        return {
            success: true,
            generatedFiles: { ...existingResults, ...newResults },
            requestedFilename: filename
        };

    } catch (error) {
        log.error({ err: error.message }, "Image processing pipeline failed");
        throw error;
    }
};

export default processImage;
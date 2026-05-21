import sharp from 'sharp';
import path from 'path';
import fs from 'fs/promises';

const OUTPUT_DIR = './public/processed';

function getOutputPath(baseName, operation) {
    const map = {
        thumbnail: `${baseName}-thumb.jpg`,
        webp: `${baseName}.webp`,
        grayscale: `${baseName}-bw.jpg`,
    };
    return path.join(OUTPUT_DIR, map[operation]);
}

async function fileExists(filePath) {
    try {
        const stats = await fs.stat(filePath);

        return stats.size > 0;
    } catch {
        return false;
    }
}

const processImage = async (payload, jobId, log) => {
    const { inputPath, filename, operations } = payload;
    const baseName = path.parse(filename).name;

    log.info({ filename, operations }, "Starting image processing");

    await fs.mkdir(OUTPUT_DIR, { recursive: true });


    // IDEMPOTENCY CHECK

    const existingResults = {};
    const pendingOperations = [];

    for (const op of operations) {
        const outputPath = getOutputPath(baseName, op);
        if (await fileExists(outputPath)) {
            // Already done on a previous attempt — include in results
            existingResults[op] = `/processed/${path.basename(outputPath)}`;
            jobLogger.info({ operation: op }, "Idempotency: Output already exists and is valid, skipping");
        } else {
            pendingOperations.push(op);
        }
    }

    // All operations already completed — nothing to do
    if (pendingOperations.length === 0) {
        jobLogger.info("All outputs already exist — returning cached results");
        return { success: true, generatedFiles: existingResults };
    }


    // Source file check

    try {
        await fs.access(inputPath);
    } catch {

        if (Object.keys(existingResults).length > 0) {
            jobLogger.warn("Source gone but partial results exist — returning what we have");
            return { success: true, generatedFiles: existingResults };
        }

        const err = new Error(`Source file not found at ${inputPath} and no outputs exist`);
        err.permanent = true;
        throw err;
    }


    // Process the pending operations

    const newResults = {};

    try {
        const imagePipeline = sharp(inputPath);

        if (pendingOperations.includes('thumbnail')) {
            const thumbPath = getOutputPath(baseName, 'thumbnail');
            await imagePipeline
                .clone()
                .resize(200, 200, { fit: 'cover' })
                .jpeg({ quality: 80 })
                .toFile(thumbPath);
            newResults.thumbnail = `/processed/${path.basename(thumbPath)}`;
        }

        if (pendingOperations.includes('webp')) {
            const webpPath = getOutputPath(baseName, 'webp');
            await imagePipeline
                .clone()
                .webp({ quality: 75 })
                .toFile(webpPath);
            newResults.webp = `/processed/${path.basename(webpPath)}`;
        }

        if (pendingOperations.includes('grayscale')) {
            const grayPath = getOutputPath(baseName, 'grayscale');
            await imagePipeline
                .clone()
                .grayscale()
                .jpeg({ quality: 90 })
                .toFile(grayPath);
            newResults.grayscale = `/processed/${path.basename(grayPath)}`;
        }


        try {
            await fs.unlink(inputPath);
            log.info("Cleaned up source file");
        } catch (cleanupError) {
            log.warn({ err: cleanupError.message }, "Failed to delete source file, but job succeeded. Proceeding.");
        }

        return {
            success: true,
            generatedFiles: { ...existingResults, ...newResults },
        };

    } catch (error) {
        jobLogger.error({ err: error.message }, "Image processing pipeline failed");
        throw error;
    }
};

export default processImage;
import sharp from 'sharp';
import path from 'path';
import fs from 'fs/promises';


const processImage = async (payload) => {
    const { inputPath, filename, operations } = payload;

    console.log(`[Image Handler] Processing: ${filename}`);

    // Ensure the input file actually exists before burning CPU
    try {
        await fs.access(inputPath);
    } catch (err) {
        throw new Error(`Input file not found at ${inputPath}`);
    }

    // Define where the finished files will go
    // In production, this would be an S3 bucket upload
    const outputDir = './public/processed'; 
    await fs.mkdir(outputDir, { recursive: true });

    // Strip the extension (e.g., 'photo.jpg' -> 'photo')
    const baseName = path.parse(filename).name;
    const results = {};

    try {
        // Initialize the Sharp instance (loads the image into memory/buffer)
        const imagePipeline = sharp(inputPath);

        //  Generate Thumbnail
        if (operations.includes('thumbnail')) {
            const thumbPath = path.join(outputDir, `${baseName}-thumb.jpg`);
            await imagePipeline
                .clone() // Clone prevents operations from affecting each other
                .resize(200, 200, { fit: 'cover' })
                .jpeg({ quality: 80 })
                .toFile(thumbPath);
            
            results.thumbnail = `/processed/${baseName}-thumb.jpg`;
        }

        //  Generate WebP
        if (operations.includes('webp')) {
            const webpPath = path.join(outputDir, `${baseName}.webp`);
            await imagePipeline
                .clone()
                .webp({ quality: 75 }) // High compression
                .toFile(webpPath);
            
            results.webp = `/processed/${baseName}.webp`;
        }

        //  Generate Grayscale
        if (operations.includes('grayscale')) {
            const grayPath = path.join(outputDir, `${baseName}-bw.jpg`);
            await imagePipeline
                .clone()
                .grayscale()
                .jpeg({ quality: 90 })
                .toFile(grayPath);
            
            results.grayscale = `/processed/${baseName}-bw.jpg`;
        }

        //  Clean up the raw original file to save disk space
        await fs.unlink(inputPath);
        console.log(`[Image Handler] Success! Cleaned up raw file.`);

        // Return the paths to the main worker loop so it can update Postgres
        return {
            success: true,
            generatedFiles: results
        };

    } catch (error) {
        console.error(`[Image Handler] Failed: ${error.message}`);
        throw error;
    }
};

export default processImage;
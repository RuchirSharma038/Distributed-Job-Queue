import axios from "axios";
import * as cheerio from "cheerio";

const processScrapper = async (payload) => {
    const { url, targetSelector } = payload;
    console.log(`[Scraper Handler] Fetching data from: ${url}`);



    try {
        // Fake the User-Agent

        // We pretend to be a real Chrome browser on a Windows machine.
        const config = {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            },
            // Set a timeout so a hanging website doesn't freeze our worker forever
            timeout: 10000
        };

        // Fetch the raw HTML
        const response = await axios.get(url, config);

        //  Load HTML into Cheerio (This acts exactly like jQuery)
        const $ = cheerio.load(response.data);

        //  Extract the data using the CSS selector
        const rawText = $(targetSelector).text().trim();

        if (!rawText) {
            throw new Error(`Selector '${targetSelector}' found no data on the page.`);
        }

        //  Data Sanitization 
        // Websites return prices as "£53.74" or "$19.99". We need a clean JavaScript Number.
        // This regex extracts only digits and the decimal point.
        const priceMatch = rawText.match(/[\d.]+/);

        if (!priceMatch) {
            throw new Error(`Could not extract a valid number from the text: ${rawText}`);
        }

        const cleanPrice = parseFloat(priceMatch[0]);
        console.log(`[Scraper Handler] Success! Found Price: ${cleanPrice}`);

        //  Return the clean data to the main worker loop
        return {
            success: true,
            extractedText: rawText,
            numericValue: cleanPrice,
            scrapedAt: new Date().toISOString()
        };

    } catch (error) {
        console.error(`[Scraper Handler] Failed: ${error.message}`);
        // Throw the error so the main worker.js updates Postgres to 'failed'
        throw error;
    }
}
export default processScrapper;
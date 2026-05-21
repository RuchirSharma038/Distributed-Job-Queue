import axios from "axios";
import * as cheerio from "cheerio";

const processScrapper = async (payload) => {
    const { url, targetSelector } = payload;
    log.info({ url, targetSelector }, "Scraper handler started");

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

        log.info({ url }, "Fetching page");

        // Fetch the raw HTML
        const response = await axios.get(url, config);

        //  Load HTML into Cheerio 
        const $ = cheerio.load(response.data);

        //  Extract the data using the CSS selector
        const rawText = $(targetSelector).text().trim();

        if (!rawText) {
            throw new Error(`Selector '${targetSelector}' found no data on the page.`);
        }
        log.info({ selector: targetSelector, rawText }, "Selector matched");

        //  Data Sanitization 

        // This regex extracts only digits and the decimal point.
        const priceMatch = rawText.match(/[\d.]+/);

        if (!priceMatch) {
            throw new Error(`Could not extract a valid number from the text: ${rawText}`);
        }

        const cleanPrice = parseFloat(priceMatch[0]);
       


        const result = {
            success: true,
            extractedText: rawText,
            numericValue: cleanPrice,
            scrapedAt: new Date().toISOString()
        };
        log.info({ numericValue: cleanPrice, extractedText: rawText }, "Scraper handler completed");

        return result;

    } catch (error) {
        log.error({ err: error.message, url }, "Scraper handler failed");
        throw error;
    }
}
export default processScrapper;
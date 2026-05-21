import processEmail from "./emailHandler.js";
import processPdf from "./pdfGenHandler.js";
import processImage from "./imageHandler.js";
import processScrapper from "./scrapperHandler.js";
import processChaos from "./processChaos.js";

export const handlers = {
    'SEND_EMAIL': processEmail,
    'PROCESS_IMAGE': processImage,
    'SCRAPE_WEBSITE': processScrapper,
    'GENERATE_PDF': processPdf,
    'TEST_CHAOS': processChaos
};
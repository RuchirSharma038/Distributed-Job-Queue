import { PRIORITIES } from "../../config/constants.js";

const jobSchemas = {
    'SEND_EMAIL': ['to', 'subject', 'body'],
    'SCRAPE_WEBSITE': ['url', 'targetSelector'],
    'PROCESS_IMAGE': ['inputPath', 'filename', 'operations'],
    'GENERATE_PDF': ['filename', 'invoiceData']
};


export const validatePayload = (jobType, payload) => {
    const requiredFields = jobSchemas[jobType];

    //  Check if the jobType exists in our schema
    if (!requiredFields) {
        const validTypes = Object.keys(jobSchemas).join(', ');
        return `Invalid jobType. Supported types are: ${validTypes}`;
    }

    //  Check if the payload contains all required fields for this specific job
    for (const field of requiredFields) {
        if (payload[field] === undefined || payload[field] === null || payload[field] === '') {
            return `Missing required parameter: '${field}' for job type '${jobType}'`;
        }
    }

    return null;
};

const MIN_SCHEDULE_BUFFER_MS = 5_000;

export const validateRunAt = (runAt) => {
    if (runAt === undefined || runAt === null) {
        return null;
    }

    const parsed = new Date(runAt);

    if (isNaN(parsed.getTime())) {
        return `'runAt' is not a valid date. Use ISO 8601 format, e.g. "2025-01-20T09:00:00.000Z"`;
    }

    const minAllowed = new Date(Date.now() + MIN_SCHEDULE_BUFFER_MS);
    if (parsed <= minAllowed) {
        return `'runAt' must be at least 5 seconds in the future. Got: ${runAt}`;
    }

    return null;
};

export const validatePriority = (priority) => {

    if (priority === undefined || priority === null) return null;

    if (!PRIORITIES.includes(priority)) {
        return `Invalid priority '${priority}'. Must be one of: ${PRIORITIES.join(', ')}`;
    }

    return null;
};

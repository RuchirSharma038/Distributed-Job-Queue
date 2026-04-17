// Define the exact required fields for each job type
const jobSchemas = {
    'SEND_EMAIL': ['to', 'subject', 'body'],
    'SCRAPE_WEBSITE': ['url', 'targetSelector'],
    'PROCESS_IMAGE': ['inputPath', 'filename', 'operations'],
    'GENERATE_PDF': ['htmlContent', 'filename']
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
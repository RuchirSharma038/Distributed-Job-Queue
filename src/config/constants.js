export const QUEUE_ROUTING = {
    'SEND_EMAIL':     'queue:io',
    'SCRAPE_WEBSITE': 'queue:io',
    'PROCESS_IMAGE':  'queue:compute',
    'GENERATE_PDF':   'queue:compute',
    'TEST_CHAOS':     'queue:io',
};
 // The sorted set key that holds delayed/retrying jobs.
// Score = timestamp (ms) at which the job becomes eligible.
export const DELAYED_QUEUE = 'queue:delayed';
 
// Exponential backoff: base delay in milliseconds.

export const RETRY_BASE_DELAY_MS = 10_000;
 
export const MAX_RETRIES_DEFAULT = 3;
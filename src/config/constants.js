export const QUEUE_ROUTING = {
    'SEND_EMAIL':     'queue:io',
    'SCRAPE_WEBSITE': 'queue:io',
    'PROCESS_IMAGE':  'queue:compute',
    'GENERATE_PDF':   'queue:compute',
    'TEST_CHAOS':     'queue:io',
};
 
export const DELAYED_QUEUE = 'queue:delayed';

export const DEAD_QUEUE = 'queue:dead';
 


export const RETRY_BASE_DELAY_MS = 10_000;
 
export const MAX_RETRIES_DEFAULT = 3;
export const QUEUE_ROUTING = {
    'SEND_EMAIL': 'queue:io',
    'SCRAPE_WEBSITE': 'queue:io',
    'PROCESS_IMAGE': 'queue:compute',
    'GENERATE_PDF': 'queue:compute',
    'TEST_CHAOS': 'queue:io',
};

export const DELAYED_QUEUE = 'queue:delayed';

export const DEAD_QUEUE = 'queue:dead';


export const PRIORITIES = ['high', 'default', 'low'];
export const DEFAULT_PRIORITY = 'default';

export function getPriorityQueue(baseQueue, priority = DEFAULT_PRIORITY) {
    return `${baseQueue}:${priority}`;
}

export function getBrpopArgs(baseQueue) {
    return PRIORITIES.map(p => getPriorityQueue(baseQueue, p));
}

export const IO_BRPOP_QUEUES = getBrpopArgs('queue:io');

export const COMPUTE_BRPOP_QUEUES = getBrpopArgs('queue:compute');

export const RETRY_BASE_DELAY_MS = 10_000;

export const MAX_RETRIES_DEFAULT = 3;
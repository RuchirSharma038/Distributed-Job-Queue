let _io = null;

export function setIo(io) {
    _io = io;
}

export function getIo() {
    if (!_io) throw new Error('Socket.io not initialized — call setIo() first');
    return _io;
}
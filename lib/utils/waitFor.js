const waitFor = async (resolveCB, interval = 1000) => {
    return new Promise((resolve) => {
        const inter = setInterval(() => {
            if (resolveCB()) {
                clearInterval(inter);
                return resolve();
            }
        }, interval);
    });
};

module.exports = {
    waitFor
};

const waitFor = async (resolveCB, interval = 1000) => {
    return new Promise((resolve) => {
        const inter = setInterval(() => { // eslint-disable-line
            if (resolveCB()) {
                clearInterval(inter);
                return resolve();
            }
        }, interval);
    });
};

const sleep = d => new Promise(r => setTimeout(r, d));

module.exports = {
    waitFor,
    sleep
};

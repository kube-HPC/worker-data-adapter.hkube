const now = require('performance-now');

const printTime = (start, method) => {
    const end = now();
    const diff = (end - start).toFixed(3);
    console.log(`${method.replace('bound ', '')} function took ${diff} ms`);
};

const wrapper = (method) => {
    const isAsync = method.constructor.name === 'AsyncFunction';
    if (isAsync) {
        return async (...args) => {
            const start = now();
            try {
                const result = await method.call(null, ...args);
                return result;
            }
            finally {
                printTime(start, method.name);
            }
        };
    }
    return (...args) => {
        const start = now();
        try {
            const result = method.call(null, ...args);
            return result;
        }
        finally {
            printTime(start, method.name);
        }
    };
};

module.exports = wrapper;

const chai = require('chai');
chai.use(require('chai-as-promised'))
const uuid = require('uuid/v4');
const { Encoding } = require('@hkube/encoding');
const expect = chai.expect
const Cache = require('../lib/cache/cache');
const config = require('./config');
const globalInput = [[3, 6, 9, 1, 5, 4, 8, 7, 2], 'asc'];
const encodedData = { data: { array: globalInput[0] }, myValue: globalInput[1] };
const storage = config.storageAdapters[config.defaultStorage];
const encoding = new Encoding({ type: storage.encoding });
const { header, payload } = encoding.encodeHeaderPayload(encodedData);
const MB = 1024 * 1024;

describe('Cache', () => {
    it('should not update due to max size', async () => {
        const taskId1 = 'taskId:' + uuid();
        const maxCacheSize = 300;
        const itemSize = 301;
        const size = itemSize * MB;
        const cache = new Cache({ maxCacheSize });
        const res = cache.update(taskId1, payload, size, header);
        expect(res).to.equal(false);
        expect(cache.size).to.equal(0);
    });
    it('should update cache until max size', async () => {
        const taskId1 = 'taskId:' + uuid();
        const taskId2 = 'taskId:' + uuid();
        const taskId3 = 'taskId:' + uuid();
        const taskId4 = 'taskId:' + uuid();
        const taskId5 = 'taskId:' + uuid();
        const taskId6 = 'taskId:' + uuid();
        const maxCacheSize = 300;
        const itemSize = 90;
        const size = itemSize * MB;
        const cache = new Cache({ maxCacheSize });
        const maxCount = Math.round(maxCacheSize / itemSize);
        cache.update(taskId1, payload, size, header);
        cache.update(taskId2, payload, size, header);
        cache.update(taskId3, payload, size, header);
        cache.update(taskId4, payload, size, header);
        cache.update(taskId5, payload, size, header);
        cache.update(taskId6, payload, size, header);
        expect(cache.count).to.equal(maxCount);
        expect(cache.size).to.equal(maxCount * size);
    });
    it('should update cache until max size', async () => {
        const taskId1 = 'taskId:' + uuid();
        const taskId2 = 'taskId:' + uuid();
        const taskId3 = 'taskId:' + uuid();
        const taskId4 = 'taskId:' + uuid();
        const maxCacheSize = 300;
        const cache = new Cache({ maxCacheSize });
        const itemSize = 90;
        const size = itemSize * MB;
        const res1 = cache.update(taskId1, payload, size, header);
        const res2 = cache.update(taskId2, payload, size, header);
        const res3 = cache.update(taskId3, payload, size, header);
        const res4 = cache.update(taskId4, payload, size * size, header);
        expect(res1).to.equal(true);
        expect(res2).to.equal(true);
        expect(res3).to.equal(true);
        expect(res4).to.equal(false);
    });
});
